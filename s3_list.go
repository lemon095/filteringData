package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3FileInfo S3文件信息结构
type S3FileInfo struct {
	Key          string // S3对象键
	Size         int64  // 文件大小
	LastModified string // 最后修改时间
	GameID       int    // 游戏ID
	Mode         string // 模式：normal 或 fb
	RtpLevel     int    // RTP等级
	TestNum      int    // 测试编号
	FbType       string // Fb类型：fb1, fb2, fb3（仅用于fb2模式）
}

// S3Client S3客户端
type S3Client struct {
	client *s3.Client
	bucket string
}

// NewS3Client 创建S3客户端
func NewS3Client(config *Config) (*S3Client, error) {
	if !config.S3.Enabled {
		return nil, fmt.Errorf("S3功能未启用")
	}

	// 尝试加载.env文件
	if err := loadEnvFile(".env"); err != nil {
		// .env文件不存在或读取失败，继续使用其他方式
		fmt.Printf("⚠️  未找到.env文件，使用配置文件或环境变量: %v\n", err)
	}

	// 配置AWS客户端
	// 优先级：环境变量 > .env文件 > 配置文件
	accessKeyID := config.S3.AccessKeyID
	secretAccessKey := config.S3.SecretAccessKey

	// 检查环境变量
	if envAccessKey := os.Getenv("AWS_ACCESS_KEY_ID"); envAccessKey != "" {
		accessKeyID = envAccessKey
	}
	if envSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY"); envSecretKey != "" {
		secretAccessKey = envSecretKey
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithRegion(config.S3.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKeyID,
			secretAccessKey,
			"", // 非临时凭证无需会话令牌
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("无法加载AWS配置: %v", err)
	}

	// 创建S3客户端
	client := s3.NewFromConfig(cfg)

	return &S3Client{
		client: client,
		bucket: config.S3.Bucket,
	}, nil
}

// CheckGameModes 检查游戏ID下有哪些模式的文件
func (s3c *S3Client) CheckGameModes(gameID int) (bool, bool, error) {
	hasNormal := false
	hasFb := false

	// 检查normal模式
	normalPrefix := fmt.Sprintf("mpg-slot-data/%d/normal/", gameID)
	normalInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s3c.bucket),
		Prefix:  aws.String(normalPrefix),
		MaxKeys: aws.Int32(1), // 只需要检查是否有文件
	}
	normalResult, err := s3c.client.ListObjectsV2(context.TODO(), normalInput)
	if err != nil {
		return false, false, fmt.Errorf("检查normal模式失败: %v", err)
	}
	hasNormal = len(normalResult.Contents) > 0

	// 检查fb模式
	fbPrefix := fmt.Sprintf("mpg-slot-data/%d/fb/", gameID)
	fbInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s3c.bucket),
		Prefix:  aws.String(fbPrefix),
		MaxKeys: aws.Int32(1), // 只需要检查是否有文件
	}
	fbResult, err := s3c.client.ListObjectsV2(context.TODO(), fbInput)
	if err != nil {
		return false, false, fmt.Errorf("检查fb模式失败: %v", err)
	}
	hasFb = len(fbResult.Contents) > 0

	return hasNormal, hasFb, nil
}

// ListS3Files 列出S3指定前缀下的所有JSON文件
func (s3c *S3Client) ListS3Files(gameIDs []int, mode string) ([]S3FileInfo, error) {
	var allFiles []S3FileInfo

	for _, gameID := range gameIDs {
		// 构建路径前缀
		var prefix string
		if mode == "fb" {
			prefix = fmt.Sprintf("mpg-slot-data/%d/fb/", gameID)
		} else {
			prefix = fmt.Sprintf("mpg-slot-data/%d/normal/", gameID)
		}

		fmt.Printf("🔍 正在搜索S3路径: %s\n", prefix)

		// 准备请求参数
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(s3c.bucket),
			Prefix: aws.String(prefix),
		}

		// 分页查询
		paginator := s3.NewListObjectsV2Paginator(s3c.client, input)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.TODO())
			if err != nil {
				return nil, fmt.Errorf("获取S3目录内容失败: %v", err)
			}

			// 收集JSON文件
			for _, obj := range page.Contents {
				key := *obj.Key
				// 只处理JSON文件
				if strings.HasSuffix(key, ".json") {
					// 解析文件名获取RTP等级和测试编号
					fileName := key[strings.LastIndex(key, "/")+1:]
					rtpLevel, testNum := parseFileName(fileName)

					fileInfo := S3FileInfo{
						Key:          key,
						Size:         *obj.Size,
						LastModified: obj.LastModified.Format("2006-01-02 15:04:05"),
						GameID:       gameID,
						Mode:         mode,
						RtpLevel:     rtpLevel,
						TestNum:      testNum,
					}
					allFiles = append(allFiles, fileInfo)
				}
			}
		}
	}

	fmt.Printf("✅ 在S3中找到 %d 个JSON文件\n", len(allFiles))
	return allFiles, nil
}

// DownloadS3File 下载S3文件内容
func (s3c *S3Client) DownloadS3File(key string) ([]byte, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s3c.bucket),
		Key:    aws.String(key),
	}

	result, err := s3c.client.GetObject(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("下载S3文件失败: %v", err)
	}
	defer result.Body.Close()

	// 读取文件内容
	body, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("读取S3文件内容失败: %v", err)
	}

	return body, nil
}

// GetObjectStream 获取S3对象的流式读取器
func (s3c *S3Client) GetObjectStream(key string) (*s3.GetObjectOutput, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s3c.bucket),
		Key:    aws.String(key),
	}

	result, err := s3c.client.GetObject(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("获取S3对象流失败: %v", err)
	}

	return result, nil
}

// parseFileName 解析文件名获取RTP等级和测试编号
// 支持两种格式：
// 1. GameResults_50_1.json -> rtpLevel=50, testNum=1
// 2. GameResultData_fb1_1_8.json -> rtpLevel=1, testNum=8
func parseFileName(fileName string) (int, int) {
	// 移除.json后缀
	name := strings.TrimSuffix(fileName, ".json")

	// 先尝试解析 Fb2 格式：GameResultData_fbType_档位_第几个文件
	re := regexp.MustCompile(`GameResultData_fb\d+_(\d+)_(\d+)`)
	matches := re.FindStringSubmatch(name)
	if len(matches) == 3 {
		rtpLevel, _ := strconv.Atoi(matches[1])
		testNum, _ := strconv.Atoi(matches[2])
		return rtpLevel, testNum
	}

	// 再尝试解析普通格式：GameResults_50_1
	parts := strings.Split(name, "_")
	if len(parts) >= 3 {
		// 尝试解析RTP等级和测试编号
		if rtpLevel, err := strconv.Atoi(parts[1]); err == nil {
			if testNum, err := strconv.Atoi(parts[2]); err == nil {
				return rtpLevel, testNum
			}
		}
	}

	return 0, 0
}

// ListS3FilesByPrefix 根据指定前缀列出S3文件
func (s3c *S3Client) ListS3FilesByPrefix(prefix string) ([]S3FileInfo, error) {
	var allFiles []S3FileInfo

	fmt.Printf("🔍 正在搜索S3路径: %s\n", prefix)

	// 准备请求参数
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(s3c.bucket),
		Prefix: aws.String(prefix),
	}

	// 分页查询
	paginator := s3.NewListObjectsV2Paginator(s3c.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("获取S3目录内容失败: %v", err)
		}

		// 收集JSON文件
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, ".json") {
				// 解析文件名获取RTP等级和测试编号
				rtpLevel, testNum := parseFileName(*obj.Key)

				// 从路径中提取游戏ID
				gameID := s3c.extractGameIDFromPath(*obj.Key)

				fileInfo := S3FileInfo{
					Key:          *obj.Key,
					Size:         *obj.Size,
					LastModified: obj.LastModified.Format(time.RFC3339),
					GameID:       gameID,
					Mode:         "fb2", // Fb2模式
					RtpLevel:     rtpLevel,
					TestNum:      testNum,
				}

				allFiles = append(allFiles, fileInfo)
			}
		}
	}

	fmt.Printf("  ✅ 找到 %d 个JSON文件\n", len(allFiles))
	return allFiles, nil
}

// extractGameIDFromPath 从S3路径中提取游戏ID
func (s3c *S3Client) extractGameIDFromPath(key string) int {
	// 路径格式：mpg-slot-data/gameID/fb/GameResultData_fbType_rtpLevel_testNum.json
	parts := strings.Split(key, "/")
	if len(parts) >= 2 {
		// 获取目录名：gameID
		dirName := parts[len(parts)-2]
		// 提取gameID
		gameIDPart := dirName
		if gameID, err := strconv.Atoi(gameIDPart); err == nil {
			return gameID
		}
	}
	return 0
}
