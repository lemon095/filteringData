package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

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
	FileMode     int    // 文件名解析出的mode值（如 0, 1, 2）
	RtpLevel     int    // RTP等级
	TestNum      int    // 测试编号
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
					// 解析文件名获取mode、RTP等级和测试编号
					fileName := key[strings.LastIndex(key, "/")+1:]
					fileMode, rtpLevel, testNum := parseFileName(fileName)

					fileInfo := S3FileInfo{
						Key:          key,
						Size:         *obj.Size,
						LastModified: obj.LastModified.Format("2006-01-02 15:04:05"),
						GameID:       gameID,
						Mode:         mode,
						FileMode:     fileMode,
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
// 例如: GameResults_50_1.json -> rtpLevel=50, testNum=1
func parseFileName(fileName string) (int, int, int) {
	// 移除.json后缀
	name := strings.TrimSuffix(fileName, ".json")

	// 按_分割
	parts := strings.Split(name, "_")

	// 支持新格式：GameResults_{mode}_{rtpLevel}_{testNum}.json
	if len(parts) >= 4 && parts[0] == "GameResults" {
		// 新格式：GameResults_2_200_1.json
		if mode, err := strconv.Atoi(parts[1]); err == nil {
			if rtpLevel, err := strconv.Atoi(parts[2]); err == nil {
				if testNum, err := strconv.Atoi(parts[3]); err == nil {
					return mode, rtpLevel, testNum
				}
			}
		}
	} else if len(parts) >= 3 {
		// 兼容旧格式：GameResults_{rtpLevel}_{testNum}.json
		// 默认mode为0
		if rtpLevel, err := strconv.Atoi(parts[1]); err == nil {
			if testNum, err := strconv.Atoi(parts[2]); err == nil {
				return 0, rtpLevel, testNum
			}
		}
	}

	return 0, 0, 0
}
