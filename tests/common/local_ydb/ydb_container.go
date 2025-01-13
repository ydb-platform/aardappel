package local_ydb

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	healthCheckTimeout   = time.Minute * 1
	containerStopTimeout = time.Minute * 2
	defaultYdbImage      = "ydbplatform/local-ydb:latest"
	grpcPort             = "2136"
	uiPort               = "8765"
)

func convertArgs(args []string) []interface{} {
	ifaces := make([]interface{}, len(args))
	for i, v := range args {
		ifaces[i] = v
	}
	return ifaces
}

func createContainerRequest(containerSettings ContainerSettings) testcontainers.ContainerRequest {
	env := map[string]string{
		"GRPC_PORT":             grpcPort,
		"YDB_DEFAULT_LOG_LEVEL": "NOTICE",
		"YDB_FEATURE_FLAGS":     "enable_implicit_query_parameter_types",
	}
	request := testcontainers.ContainerRequest{
		Image:        containerSettings.YdbImage,
		ExposedPorts: []string{containerSettings.GrpcPort.Port(), containerSettings.UIPort.Port()},
		WaitingFor: wait.ForAll(
			wait.ForHealthCheck().WithStartupTimeout(healthCheckTimeout),
		),
		Env:           env,
		ImagePlatform: "linux/amd64",
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      "./ydbctl.sh",
				ContainerFilePath: "/initialize_local_ydb",
				FileMode:          777,
			},
		},
	}

	request.HostConfigModifier = func(hc *container.HostConfig) {
		hc.Binds = make([]string, 0)
		for containerPath, localPath := range containerSettings.Mounts {
			hc.Binds = append(hc.Binds, fmt.Sprintf("%s:%s", localPath, containerPath))
		}
	}

	return request
}

type ContainerSettings struct {
	YdbImage string
	Mounts   map[string]string
	GrpcPort nat.Port
	UIPort   nat.Port
}

func getContainerSettings(settings YdbSettings, ydbDir *YDBDir) ContainerSettings {
	var result ContainerSettings
	result.Mounts = make(map[string]string, 0)
	if settings.YdbImage != nil {
		result.YdbImage = *settings.YdbImage
	} else if ydbImage := os.Getenv("YDB_IMAGE"); ydbImage != "" {
		result.YdbImage = ydbImage
	} else {
		result.YdbImage = defaultYdbImage
	}
	fmt.Println("YDB_IMAGE: ", result.YdbImage)

	if ydbDir != nil {
		result.Mounts["/ydb_certs"] = ydbDir.DirYdbCerts
		result.Mounts["/ydb_data"] = ydbDir.DirYdbData
	}

	result.GrpcPort, _ = nat.NewPort("tcp", grpcPort)
	result.UIPort, _ = nat.NewPort("tcp", uiPort)

	return result
}

type YDBDir struct {
	path        string
	DirYdbData  string
	DirYdbCerts string
}

func NewYDBDir() (*YDBDir, error) {
	dir, err := os.MkdirTemp("", "ydb-test-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create tmp directory: %w", err)
	}
	log.Println(fmt.Sprintf("test dir: %s", dir))
	certsDir := filepath.Join(dir, "ydb_certs")
	err = os.Mkdir(certsDir, 777)
	if err != nil {
		return nil, fmt.Errorf("failed to create ydb-certs directory: %w", err)
	}
	dataDir := filepath.Join(dir, "ydb_data")
	err = os.Mkdir(dataDir, 777)
	if err != nil {
		return nil, fmt.Errorf("failed to create ydb-data directory: %w", err)
	}
	return &YDBDir{
		path:        dir,
		DirYdbData:  dataDir,
		DirYdbCerts: certsDir,
	}, nil
}

func (ydbDir *YDBDir) DeleteDir() error {
	log.Println("Deleting temporary directory...")
	err := os.RemoveAll(ydbDir.path)
	if err != nil {
		return fmt.Errorf("failed to delete test directory: %w", err)
	}
	log.Println("Deleted temporary directory successfully")
	return nil
}

type ConnectInfo struct {
	GrpcEndpoint string
	UIEndpoint   string
}

type YDBContainer struct {
	container   testcontainers.Container
	YDBDir      *YDBDir
	ConnectInfo ConnectInfo
}

func NewYDBContainer(ctx context.Context, ydbSettings YdbSettings) (*YDBContainer, error) {
	var ydbContainer testcontainers.Container
	var ydbDir *YDBDir
	var err error

	if ydbSettings.OnDisk {
		ydbDir, err = NewYDBDir()
		if err != nil {
			return nil, fmt.Errorf("failed to create container: %w", err)
		}
	}

	containerSettings := getContainerSettings(ydbSettings, ydbDir)
	containerRequest := createContainerRequest(containerSettings)

	ydbContainer, err = testcontainers.GenericContainer(
		ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: containerRequest,
			Started:          true,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error creating YDB test container: %w", err)
	}

	grpcEndpoint, err := ydbContainer.PortEndpoint(ctx, containerSettings.GrpcPort, "")
	if err != nil {
		return nil, fmt.Errorf("error getting YDB port: %s, error: %w", containerSettings.GrpcPort, err)
	}
	uiEndpoint, err := ydbContainer.PortEndpoint(ctx, containerSettings.UIPort, "")
	if err != nil {
		return nil, fmt.Errorf("error getting YDB port: %s, error: %w", containerSettings.UIPort, err)
	}

	return &YDBContainer{
		container: ydbContainer,
		YDBDir:    ydbDir,
		ConnectInfo: ConnectInfo{
			GrpcEndpoint: grpcEndpoint,
			UIEndpoint:   uiEndpoint,
		},
	}, nil
}

func (ydbContainer *YDBContainer) execute(ctx context.Context, cmd string, args []string, options ...tcexec.ProcessOption) (string, error) {
	if ydbContainer.container == nil {
		return "", fmt.Errorf("error to exec command in container: container is nil")
	}

	commandFormatted := fmt.Sprintf(cmd, convertArgs(args)...)
	execCmd := []string{"sh", "-c", commandFormatted}

	var returnErr error
	code, reader, err := ydbContainer.container.Exec(ctx, execCmd, options...)
	if err != nil {
		return "", fmt.Errorf("failed to exec command %v in container: %w", commandFormatted, err)
	}

	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	if err != nil {
		return "", fmt.Errorf("failed to read command output during execution in container: %w", err)
	}

	if code != 0 {
		returnErr = fmt.Errorf("command %v returned non-zero code %d during execution in container", commandFormatted, code)
	}

	return buf.String(), returnErr
}

func (ydbContainer *YDBContainer) chmodAllFiles(ctx context.Context) error {
	output, err := ydbContainer.execute(ctx, "chmod -R 777 %s %s", []string{"/ydb_certs", "/ydb_data"})
	if err != nil {
		return fmt.Errorf("failed running chmod for ydb_certs and ydb_data in container: %w, output: %s", err, output)
	}
	if output != "" {
		return fmt.Errorf("chmod failed: %s", output)
	}
	return nil
}

func (ydbContainer *YDBContainer) Terminate(ctx context.Context) error {
	if ydbContainer.YDBDir != nil {
		err := ydbContainer.chmodAllFiles(ctx)
		if err != nil {
			return fmt.Errorf("failed to terminate container during running chmod for files in container: %w", err)
		}
		err = ydbContainer.YDBDir.DeleteDir()
		if err != nil {
			return fmt.Errorf("failed to terminate container during deleting container: %w", err)
		}
	}
	log.Println("Stopping YDB container...")
	timeout := containerStopTimeout
	return ydbContainer.container.Stop(ctx, &timeout)
}

func (y *YDBContainer) RestartWithDowntime(ctx context.Context, downtimeSec int) error {
	log.Println("Restarting YDB container...")
	output, err := y.execute(ctx, "/initialize_local_ydb %s %s", []string{"restart", fmt.Sprint(downtimeSec)})
	if err != nil {
		return fmt.Errorf("failed to restart YDB: %w, output: %s", err, output)
	}
	log.Println("YDB restarteed successfully")
	return nil
}

func (y *YDBContainer) Restart(ctx context.Context) error {
	return y.RestartWithDowntime(ctx, 0)
}
