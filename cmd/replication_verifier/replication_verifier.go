package main

import (
	"aardappel/internal/util/xlog"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type TablePair struct {
	Source  string `yaml:"source"`
	Replica string `yaml:"replica"`
}

type Config struct {
	SrcConnectionString string      `yaml:"src_connection_string"`
	SrcOAuthFile        string      `yaml:"src_oauth2_file"`
	SrcStaticToken      string      `yaml:"src_static_token"`
	SrcClientBalancer   bool        `yaml:"src_client_balancer"`
	DstConnectionString string      `yaml:"dst_connection_string"`
	DstOAuthFile        string      `yaml:"dst_oauth2_file"`
	DstStaticToken      string      `yaml:"dst_static_token"`
	DstClientBalancer   bool        `yaml:"dst_client_balancer"`
	SleepBetweenDiffs   string      `yaml:"sleep_between_diffs"` // это время на то, чтобы транзакции, которые сейчас в полете, успели долиться
	BatchSize           int         `yaml:"batch_size"`          // число строк в селекте за раз
	OutputDir           string      `yaml:"output_dir"`          // сюда кладем файлы с результатами - на каждую таблицу по 3 файла: есть в src и нет в реплике; наоборот; есть везде, но отличается
	Tables              []TablePair `yaml:"tables"`              // перечисляем таблицы для сравнения
}

type TableSchema struct {
	Path       string
	PK         []string
	Columns    []Column
	ColIndex   map[string]Column
	Comparable []string // берем только общие колонки для сравнения
}

type Column struct {
	Name     string
	Type     ydb_types.Type
	TypeName string
}

type Row struct {
	Key    []any          // значения PK по порядку
	Data   map[string]any // только сравниваемые колонки
	Sig    uint64         // хеш от Data для быстрой проверки неравенства строки
	Raw    map[string]any // все общие колонки
	KeyStr string         // строковый ключ для map (для пересечения проходов)
}

type DiffSets struct {
	SourceOnly  map[string]Row     // есть в source, нет в replica
	Mismatch    map[string]RowPair // есть везде, но отличаются
	ReplicaOnly map[string]Row     // есть в replica, нет в source
}

type RowPair struct {
	Src Row
	Rep Row
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 2000
	}
	if c.OutputDir == "" {
		c.OutputDir = "./out"
	}
	return &c, nil
}

func createYdbDriverAuthOptions(oauthFile string, staticToken string) ([]ydb.Option, error) {
	if (len(oauthFile) > 0 && len(staticToken) > 0) || (len(oauthFile) == 0 && len(staticToken) == 0) {
		return nil, errors.New("it's either oauth2_file or static_token option must be set")
	}

	if len(oauthFile) > 0 {
		return []ydb.Option{
			ydb.WithOauth2TokenExchangeCredentialsFile(oauthFile),
		}, nil
	}

	if len(staticToken) > 0 {
		return []ydb.Option{
			ydb.WithAccessTokenCredentials(staticToken),
		}, nil
	}
	return nil, errors.New("not supported")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: reconcile <config.yaml>")
		os.Exit(2)
	}
	cfg, err := loadConfig(os.Args[1])
	checkErr(err)

	ctx := context.Background()

	srcOpts, err := createYdbDriverAuthOptions(cfg.SrcOAuthFile, cfg.SrcStaticToken)
	if err != nil {
		xlog.Fatal(ctx, "Unable to create auth option for src",
			zap.Error(err))
	}

	dstOpts, err := createYdbDriverAuthOptions(cfg.DstOAuthFile, cfg.DstStaticToken)
	if err != nil {
		xlog.Fatal(ctx, "Unable to create auth option for dst",
			zap.Error(err))
	}

	if cfg.SrcClientBalancer == false {
		srcOpts = append(srcOpts, ydb.WithBalancer(balancers.SingleConn()))
	}

	if cfg.DstClientBalancer == false {
		dstOpts = append(dstOpts, ydb.WithBalancer(balancers.SingleConn()))
	}

	srcDB, err := ydb.Open(ctx, cfg.SrcConnectionString, srcOpts...)
	checkErr(err)
	defer srcDB.Close(ctx)

	repDB, err := ydb.Open(ctx, cfg.DstConnectionString, dstOpts...)
	checkErr(err)
	defer repDB.Close(ctx)

	checkErr(os.MkdirAll(cfg.OutputDir, 0o755))

	srcTable := srcDB.Table()
	repTable := repDB.Table()
	srcScript := srcDB.Scripting()
	repScript := repDB.Scripting()

	// первый проход: собираем разницу для всех таблиц
	log.Println("Step #1: scanning differences...")
	first := make(map[string]DiffSets)
	for _, tp := range cfg.Tables {
		ds, err := diffTable(ctx, tp, srcTable, repTable, srcScript, repScript, cfg.BatchSize)
		checkErr(err)
		first[keyPair(tp)] = ds
	}

	// ждем
	sleepDur, _ := time.ParseDuration(cfg.SleepBetweenDiffs)
	if sleepDur > 0 {
		log.Printf("Sleeping %s to allow replication catch-up...\n", sleepDur)
		time.Sleep(sleepDur)
	}

	// второй проход
	log.Println("Step #2: scanning differences...")
	second := make(map[string]DiffSets)
	for _, tp := range cfg.Tables {
		ds, err := diffTable(ctx, tp, srcTable, repTable, srcScript, repScript, cfg.BatchSize)
		checkErr(err)
		second[keyPair(tp)] = ds
	}

	// Выводим разницу таблиц, которая сохранилась после паузы
	log.Println("Intersecting stable differences and writing CSV...")
	for _, tp := range cfg.Tables {
		name := keyPair(tp)
		a := first[name]
		b := second[name]

		lostInsert := intersectRowSets(a.SourceOnly, b.SourceOnly)
		lostDelete := intersectRowSets(a.ReplicaOnly, b.ReplicaOnly)
		mismatch := intersectPairSets(a.Mismatch, b.Mismatch)

		// файлы: добавим имя таблицы
		base := sanitize(strings.ReplaceAll(name, " -> ", "__"))
		checkErr(writeRowsCSV(filepath.Join(cfg.OutputDir, base+"__lost_insert.csv"), lostInsert))
		checkErr(writePairsCSV(filepath.Join(cfg.OutputDir, base+"__mismatch.csv"), mismatch))
		checkErr(writeRowsCSV(filepath.Join(cfg.OutputDir, base+"__lost_delete.csv"), lostDelete))
	}

	log.Println("Done")
}

func keyPair(tp TablePair) string { return tp.Source + " -> " + tp.Replica }

func sanitize(s string) string {
	r := strings.NewReplacer("/", "_")
	return r.Replace(s)
}

// сравнение 1 таблицы

const DEFAULT_TIMEOUT = 5 * time.Second // таймаут на операции с YDB

func diffTable(
	ctx context.Context,
	tp TablePair,
	srcTable table.Client,
	repTable table.Client,
	srcScript scripting.Client,
	repScript scripting.Client,
	batch int,
) (DiffSets, error) {

	ctxWithTimeout, cancel := context.WithTimeout(ctx, DEFAULT_TIMEOUT)
	defer cancel()

	// получаем схему таблиц
	srcScheme, err := describeTable(ctxWithTimeout, srcTable, tp.Source)
	if err != nil {
		return DiffSets{}, fmt.Errorf("describe source %s: %w", tp.Source, err)
	}
	ctxWithTimeout, _ = context.WithTimeout(ctx, DEFAULT_TIMEOUT)
	repScheme, err := describeTable(ctxWithTimeout, repTable, tp.Replica)
	if err != nil {
		return DiffSets{}, fmt.Errorf("describe replica %s: %w", tp.Replica, err)
	}

	// Сравниваемые колонки - все общие колонки в оригинальной таблице и в реплике
	compCols := intersectColumns(srcScheme, repScheme)
	srcScheme.Comparable = compCols
	repScheme.Comparable = compCols

	// Бежим по обеим таблицам дочитывая batch строк и сравнивая между собой
	left := newPager(srcScript, srcScheme, batch)
	right := newPager(repScript, repScheme, batch)

	result := DiffSets{
		SourceOnly:  make(map[string]Row),
		ReplicaOnly: make(map[string]Row),
		Mismatch:    make(map[string]RowPair),
	}

	var (
		srcBuf         []Row
		repBuf         []Row
		srcEOF, repEOF bool
		srcIdx, repIdx int
	)

	// первый select
	srcBuf, srcEOF, readErr := left.next(ctx)
	checkErr(readErr)
	repBuf, repEOF, readErr = right.next(ctx)
	checkErr(readErr)

	for {
		// если оба закончились — выходим
		if (srcEOF && srcIdx >= len(srcBuf)) && (repEOF && repIdx >= len(repBuf)) {
			break
		}
		// если закончился буфер слева — подгружаем
		if srcIdx >= len(srcBuf) && !srcEOF {
			srcBuf, srcEOF, readErr = left.next(ctx)
			checkErr(readErr)
			srcIdx = 0
		}
		// если закончился буфер справа — подгружаем
		if repIdx >= len(repBuf) && !repEOF {
			repBuf, repEOF, readErr = right.next(ctx)
			checkErr(readErr)
			repIdx = 0
		}

		if srcIdx >= len(srcBuf) && !srcEOF {
			continue
		}
		if repIdx >= len(repBuf) && !repEOF {
			continue
		}
		// если один поток пуст, другой не пуст — относим в только в репликации или только в источнике
		if srcIdx >= len(srcBuf) && srcEOF && repIdx < len(repBuf) {
			// всё оставшееся справа — только в реплике
			for ; repIdx < len(repBuf); repIdx++ {
				r := repBuf[repIdx]
				result.ReplicaOnly[r.KeyStr] = r
			}
			continue
		}
		if repIdx >= len(repBuf) && repEOF && srcIdx < len(srcBuf) {
			// всё оставшееся слева — только в источнике
			for ; srcIdx < len(srcBuf); srcIdx++ {
				r := srcBuf[srcIdx]
				result.SourceOnly[r.KeyStr] = r
			}
			continue
		}

		// сравнение текущих ключей
		var (
			hasSrc = srcIdx < len(srcBuf)
			hasRep = repIdx < len(repBuf)
		)
		// проверка на всякий случай
		if !hasSrc && !hasRep {
			continue
		}
		// есть только в источнике
		if hasSrc && !hasRep {
			r := srcBuf[srcIdx]
			result.SourceOnly[r.KeyStr] = r
			srcIdx++
			continue
		}
		// есть только в реплике
		if !hasSrc && hasRep {
			r := repBuf[repIdx]
			result.ReplicaOnly[r.KeyStr] = r
			repIdx++
			continue
		}

		srсRow := srcBuf[srcIdx]
		replicaRow := repBuf[repIdx]
		cmp := compareKeys(srсRow.Key, replicaRow.Key)
		switch {
		case cmp < 0:
			// ключ есть в источнике, нет в реплике
			result.SourceOnly[srсRow.KeyStr] = srсRow
			srcIdx++
		case cmp > 0:
			// ключ есть в реплике, нет в источнике
			result.ReplicaOnly[replicaRow.KeyStr] = replicaRow
			repIdx++
		default:
			// ключи равны: сравниваем значения хэша от общих колонок
			if srсRow.Sig != replicaRow.Sig {
				result.Mismatch[srсRow.KeyStr] = RowPair{Src: srсRow, Rep: replicaRow}
			}
			srcIdx++
			repIdx++
		}
	}

	return result, nil
}

// получение схемы аблицы в ydb
func ydbDescribeTable(ctx context.Context, client table.Client, tablePath string) (*options.Description, error) {
	var desc options.Description
	err := client.Do(ctx, func(ctx context.Context, s table.Session) error {
		var err error
		desc, err = s.DescribeTable(ctx, tablePath)
		return err
	}, table.WithIdempotent())
	if err != nil {
		xlog.Error(ctx, "Unable to describe table", zap.Error(err))
		return nil, fmt.Errorf("DescribeTable: %w", err)
	}
	return &desc, nil
}

// получение схемы таблицы со всеми необходимыми данными
func describeTable(ctx context.Context, tc table.Client, path string) (*TableSchema, error) {
	desc, err := ydbDescribeTable(ctx, tc, path)
	if err != nil {
		return nil, fmt.Errorf("DescribeTable %s: %w", path, err)
	}
	ts := &TableSchema{
		Path:     path,
		PK:       desc.PrimaryKey,
		Columns:  make([]Column, 0, len(desc.Columns)),
		ColIndex: make(map[string]Column),
	}
	for _, c := range desc.Columns {
		col := Column{
			Name:     c.Name,
			TypeName: c.Type.Yql(),
			Type:     c.Type,
		}
		ts.Columns = append(ts.Columns, col)
		ts.ColIndex[c.Name] = col
	}
	return ts, nil
}

// получение пересечения двух наборов столбцов
func intersectColumns(src, replica *TableSchema) []string {
	m := map[string]struct{}{}
	for _, c := range replica.Columns {
		if _, ok := src.ColIndex[c.Name]; ok {
			m[c.Name] = struct{}{}
		}
	}

	for _, k := range replica.PK {
		m[k] = struct{}{}
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// структура для подчитывания нового batcha строк из таблицы
type pager struct {
	script   scripting.Client
	scheme   *TableSchema
	batch    int
	afterKey []any
	eof      bool
}

func newPager(sc scripting.Client, scheme *TableSchema, batch int) *pager {
	return &pager{script: sc, scheme: scheme, batch: batch}
}

func (p *pager) next(ctx context.Context) ([]Row, bool, error) {
	if p.eof {
		return nil, true, nil
	}
	cols := append([]string{}, p.scheme.Comparable...)
	// в начале берем первичный ключ, потом остальные колонки
	pkSet := map[string]bool{}
	for _, k := range p.scheme.PK {
		pkSet[k] = true
	}
	sort.Slice(cols, func(i, j int) bool {
		_, ip := pkSet[cols[i]]
		_, jp := pkSet[cols[j]]
		if ip != jp {
			return ip
		}
		return cols[i] < cols[j]
	})

	selectList := strings.Join(cols, ", ")
	orderBy := strings.Join(p.scheme.PK, ", ")

	where := ""
	params := make([]table.ParameterOption, 0)
	limitParam, _ := convertToYDBValue(p.batch, ydb_types.TypeUint64)
	params = append(params, table.ValueParam("$limit", limitParam))

	// строим условие для where по последнему прочитанному ключу
	if p.afterKey != nil {
		// (k1 > $k1) OR (k1 = $k1 AND k2 > $k2) OR ...
		var terms []string // все условия для where
		for i := range p.scheme.PK {
			var eqs []string // для =
			for j := 0; j < i; j++ {
				eqs = append(eqs, fmt.Sprintf("`%s` = $k%d", p.scheme.PK[j], j+1))
			}
			gt := fmt.Sprintf("`%s` > $k%d", p.scheme.PK[i], i+1) // для >
			if len(eqs) > 0 {
				terms = append(terms, fmt.Sprintf("(%s AND %s)", strings.Join(eqs, " AND "), gt))
			} else {
				terms = append(terms, gt)
			}
			paramValue, _ := convertToYDBValue(p.afterKey[i], p.scheme.ColIndex[p.scheme.PK[i]].Type)
			params = append(params, table.ValueParam(fmt.Sprintf("$k%d", i+1), paramValue))
		}
		where = "WHERE " + strings.Join(terms, " OR ")
	}

	declare := "DECLARE $limit AS Uint64;\n"
	for i := 1; i <= len(params)-1; i++ {
		declare += fmt.Sprintf("		DECLARE $k%d AS %s;\n", i, p.scheme.ColIndex[p.scheme.PK[i-1]].TypeName) + "\n"
	}

	columnsList := make([]string, 0, len(p.scheme.Comparable))
	for _, c := range p.scheme.Comparable {
		columnsList = append(columnsList, "\""+c+"\"")
	}
	columnsListStr := strings.Join(columnsList, ", ")

	yql := fmt.Sprintf(`
		%s
		SELECT %s, Digest::MurMurHash2A(Pickle(ChooseMembers(TableRow(), [%s]))) AS `+"sig"+`
		FROM `+"`"+`%s`+"`"+`
		%s
		ORDER BY %s
		LIMIT $limit;
	`, declare, selectList, columnsListStr, p.scheme.Path, where, orderBy)

	ctxWithTimeout, cancel := context.WithTimeout(ctx, DEFAULT_TIMEOUT)
	defer cancel()
	res, err := p.script.Execute(ctxWithTimeout, yql, table.NewQueryParameters(params...))
	if err != nil {
		fmt.Println("Couldnt execute:", err)
		return nil, false, err
	}
	defer res.Close()

	var rows []Row
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			// мапы для хранения значений колонок
			raw := make(map[string]any)
			data := make(map[string]any)
			key := make([]any, len(p.scheme.PK))

			// вспомогательная штука для scan из строки
			ptrs := make(map[string]**any)

			// читаем все колонки из selectList
			values := make([]named.Value, 0)
			for _, c := range strings.Split(selectList, ",") {
				col := strings.TrimSpace(c)
				var v *any
				ptrs[col] = &v
				values = append(values, named.Optional(col, &v))
			}
			var sig uint64
			values = append(values, named.Required("sig", &sig))

			// заполняем все колонки
			if err := res.ScanNamed(values...); err != nil {
				fmt.Println("Can't scan named")
				return nil, false, err
			}

			// распаковываем значения из вспомогательной мапы
			for col, pv := range ptrs {
				p := *pv
				if p == nil {
					raw[col] = nil
					data[col] = nil
					continue
				}
				raw[col] = *p
				data[col] = *p
			}

			// собираем key по порядку
			for i, k := range p.scheme.PK {
				key[i] = raw[k]
			}
			keyStr := keyToString(key)
			rows = append(rows, Row{
				Key: key, Data: data, Raw: raw, KeyStr: keyStr, Sig: sig,
			})
		}
	}

	if err := res.Err(); err != nil {
		return nil, false, err
	}
	if len(rows) < p.batch {
		p.eof = true
	}
	// сдвигаем курсор
	if len(rows) > 0 {
		p.afterKey = rows[len(rows)-1].Key
	}
	return rows, p.eof, nil
}

// взяла конвертор из aardappel для формирования параметра для запроса
func convertToYDBValue(value any, t ydb_types.Type) (ydb_types.Value, error) {
	var err error
	v, err := json.Marshal(value)
	if err != nil {
		checkErr(fmt.Errorf("ConvertToYDBValue: error marshal value: %w", err))
	}
	if isOptional, innerType := ydb_types.IsOptional(t); isOptional {
		var nulValue interface{}
		if err = json.Unmarshal(v, &nulValue); err == nil {
			if nulValue == nil {
				return ydb_types.NullValue(innerType), nil
			}
		}
		if err != nil {
			return nil, fmt.Errorf("ConvertToYDBValue: error unmurshal value: %w", err)
		}

		v, err := convertToYDBValue(value, innerType)
		if err != nil {
			if err != nil {
				return nil, fmt.Errorf("ConvertToYDBValue: error get optional value with inner type: %w", err)
			}
			return nil, err
		}
		return ydb_types.OptionalValue(v), nil
	}

	switch t {
	case ydb_types.TypeBool:
		var value bool
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.BoolValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to bool value: %w", err)
	case ydb_types.TypeInt8:
		var value int8
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Int8Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to int8 value: %w", err)
	case ydb_types.TypeInt16:
		var value int16
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Int16Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to int16 value: %w", err)
	case ydb_types.TypeInt32:
		var value int32
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Int32Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to int32 value: %w", err)
	case ydb_types.TypeInt64:
		var value int64
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Int64Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to int64 value: %w", err)
	case ydb_types.TypeUint8:
		var value uint8
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Uint8Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uint8 value: %w", err)
	case ydb_types.TypeUint16:
		var value uint16
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Uint16Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uint16 value: %w", err)
	case ydb_types.TypeUint32:
		var value uint32
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Uint32Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uint32 value: %w", err)
	case ydb_types.TypeUint64:
		var value uint64
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.Uint64Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uint64 value: %w", err)
	case ydb_types.TypeFloat:
		var value float32
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.FloatValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to float value: %w", err)
	case ydb_types.TypeDouble:
		var value float64
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.DoubleValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to double value: %w", err)
	case ydb_types.TypeDate:
		var value uint32
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.DateValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to date value: %w", err)
	case ydb_types.TypeTimestamp:
		var value_str string
		if err = json.Unmarshal(v, &value_str); err != nil {
			return nil, fmt.Errorf("ConvertToYDBValue: timestamp: unmurshal to string value: %w", err)
		}
		format := "2006-01-02T15:04:05.000000Z"
		value, err := time.Parse(format, value_str)
		if err != nil {
			return nil, fmt.Errorf("ConvertToYDBValue: timestamp: error to time parse: %w", err)
		}
		return ydb_types.TimestampValue(uint64(value.UnixMicro())), nil
	case ydb_types.TypeInterval:
		var value int64
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.IntervalValueFromMicroseconds(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to interval value: %w", err)
	case ydb_types.TypeTzDate:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.TzDateValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to tz date value: %w", err)
	case ydb_types.TypeTzDatetime:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.TzDatetimeValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to tz datetime value: %w", err)
	case ydb_types.TypeTzTimestamp:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.TzTimestampValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to tz timestamp value: %w", err)
	case ydb_types.TypeString:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			data, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				return nil, fmt.Errorf("DecodeString: unable to decode base64: %w", err)
			}
			return ydb_types.BytesValue(data), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to string value: %w", err)
	case ydb_types.TypeUTF8:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.UTF8Value(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to utf8 value: %w", err)
	case ydb_types.TypeYSON:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			return ydb_types.YSONValue(value), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to yson value: %w", err)
	case ydb_types.TypeJSON:
		j, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("ConvertToYDBValue: marshal to json value: %w", err)
		}
		return ydb_types.JSONValueFromBytes(j), nil
	case ydb_types.TypeUUID:
		var value string
		if err = json.Unmarshal(v, &value); err == nil {
			u, err := uuid.Parse(value)
			if err != nil {
				return nil, fmt.Errorf("ConvertToYDBValue: convert to uuid value: %w", err)
			}
			return ydb_types.UuidValue(u), nil
		}
		return nil, fmt.Errorf("ConvertToYDBValue: unmurshal to uuid value: %w", err)
	}
	return nil, fmt.Errorf("ConvertToYDBValue: unsupported type: %v", t)
}

func keyToString(key []any) string {
	parts := make([]string, len(key))
	for i, v := range key {
		parts[i] = fmt.Sprintf("%v", v)
	}
	return strings.Join(parts, "|")
}

// для сравнения клбчей
func compareKeys(a, b []any) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		c := compareScalars(a[i], b[i])
		if c != 0 {
			return c
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// сичла сравниваем как числа, остальное - строково
func compareScalars(a, b any) int {
	switch av := a.(type) {
	case int64:
		bv, _ := toInt64(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case uint64:
		bv, _ := toUint64(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv := fmt.Sprintf("%v", b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	default:
		as := fmt.Sprintf("%v", a)
		bs := fmt.Sprintf("%v", b)
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}
}

func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int32:
		return int64(t), true
	case uint64:
		if t > uint64(int64(0)) {
			return 0, false
		}
		return int64(t), true
	}
	return 0, false
}

func toUint64(v any) (uint64, bool) {
	switch t := v.(type) {
	case uint64:
		return t, true
	case int64:
		if t < 0 {
			return 0, false
		}
		return uint64(t), true
	}
	return 0, false
}

func rowsEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		if !valuesEqual(va, vb) {
			return false
		}
	}
	return true
}

func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if (a == nil) != (b == nil) {
		return false
	}
	ta := fmt.Sprintf("%v", a)
	tb := fmt.Sprintf("%v", b)
	if ta == tb {
		return true
	}
	return reflect.DeepEqual(a, b)
}

// нахождение общих строк в 2-х разницах между таблицами после паузы
func intersectRowSets(a, b map[string]Row) map[string]Row {
	out := make(map[string]Row)
	for k, v := range a {
		if _, ok := b[k]; ok {
			out[k] = v
		}
	}
	return out
}

// нахождение несовпадающих пар дата колонок в 2-х разницах между таблицами после паузы
func intersectPairSets(a, b map[string]RowPair) map[string]RowPair {
	out := make(map[string]RowPair)
	for k, v := range a {
		if w, ok := b[k]; ok {
			if !rowsEqual(w.Src.Data, w.Rep.Data) {
				out[k] = v
			} else {
				// иначе считаем, что исправилось за паузу
			}
		}
	}
	return out
}

// вывод разницы в CSV, где данные только в одной из таблиц
func writeRowsCSV(path string, m map[string]Row) error {
	if len(m) == 0 {
		// создадим пустой для консистентности
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		w := csv.NewWriter(f)
		w.Write([]string{"key", "columns"}) // заголовок
		w.Flush()
		return w.Error()
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	// заголовки: key + список колонок по алфавиту
	var cols []string
	for _, r := range m {
		for c := range r.Raw {
			cols = append(cols, c)
		}
		break
	}
	sort.Strings(cols)
	header := append([]string{"key"}, cols...)
	checkErr(w.Write(header))

	// строки
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := m[k]
		row := make([]string, 0, 1+len(cols))
		row = append(row, r.KeyStr)
		for _, c := range cols {
			row = append(row, fmt.Sprintf("%v", r.Raw[c]))
		}
		checkErr(w.Write(row))
	}
	return w.Error()
}

// вывод разницы в CSV, где данные есть в обеих таблицах, но отличаются
func writePairsCSV(path string, m map[string]RowPair) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	// Заголовок: key + "col_src" + "col_rep"
	var cols []string
	for _, rp := range m {
		for c := range rp.Src.Raw {
			cols = append(cols, c)
		}
		break
	}
	sort.Strings(cols)
	header := []string{"key"}
	for _, c := range cols {
		header = append(header, c+"_src")
		header = append(header, c+"_rep")
	}
	checkErr(w.Write(header))

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		rp := m[k]
		row := []string{k}
		for _, c := range cols {
			row = append(row, fmt.Sprintf("%v", rp.Src.Raw[c]))
			row = append(row, fmt.Sprintf("%v", rp.Rep.Raw[c]))
		}
		checkErr(w.Write(row))
	}
	return w.Error()
}
