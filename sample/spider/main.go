package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type SpinResponse struct {
	Dt struct {
		Si json.RawMessage `json:"si"`
	} `json:"dt"`
	Err json.RawMessage `json:"err"`
}

type Si struct {
	Aw  float64            `json:"aw"`
	Bl  float64            `json:"bl"`
	Ge  []int              `json:"ge"`
	Lw  map[string]float64 `json:"lw"`
	Sid string             `json:"sid"`
	St  int                `json:"st"`
	Tbb float64            `json:"tbb"`
	Nst int                `json:"nst"`
	Fs  json.RawMessage    `json:"fs"`
}

type Row struct {
	Bet    float64           `json:"bet"`
	Win    float64           `json:"win"`
	Detail []json.RawMessage `json:"detail"`
}

type Config struct {
	// ID int `yaml:"id"`
	// GenRTP     bool `yaml:"genRtp"`
	// MaxUserNum int  `yaml:"maxUserNum"`
	// AtkList    []string `yaml:"atk"`
	// Cs          float32      `yaml:"cs"`
	// Ml          int          `yaml:"ml"`
	// Fb          int          `yaml:"fb"`
	// Spin        string       `yaml:"spin"`
	// SpinAction  bool         `yaml:"spinAction"`
	BatchSpider []SpiderGame `yaml:"batchSpider"`
}

type SpiderGame struct {
	GameId     int     `yaml:"gameId"`
	Cs         float32 `yaml:"cs"`
	Ml         int     `yaml:"ml"`
	Fb         int     `yaml:"fb"`
	Spin       string  `yaml:"spin"`
	UserNum    int     `yaml:"userNum"`
	FbUserNum  int     `yaml:"fbUserNum"`
	SpinStatus bool    `yaml:"spinStatus"`
	FbStatus   bool    `yaml:"fbStatus"`
}

type RunCache struct {
	AtkList []string `yaml:"atk"`
}

func readYAML(path string, out interface{}) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	return yaml.NewDecoder(f).Decode(out)
}
func main() {
	// 读取命令行命令 Id（游戏id） genRtp （true、fasle）
	configPath := flag.String("config", "config.yaml", "配置文件路径")

	// 解析参数
	flag.Parse()

	// 读取 YAML 配置
	var cfg Config
	if err := readYAML(*configPath, &cfg); err != nil {
		log.Fatalf("❌ 读取配置失败: %v", err)
	}

	// atks := []string{}
	// for i := 0; i < 10; i++ {
	// 	atk, _ := getParamsFromFortuneSlot()
	// 	atks = append(atks, atk)
	// }
	// fmt.Println("获取到 atk 列表:", atks)

	for _, game := range cfg.BatchSpider {
		fmt.Println("🎮 Game ID:", game.GameId)

		// 普通采集
		if game.SpinStatus {
			outputFile := fmt.Sprintf("game_%d.csv", game.GameId)
			file, err := os.Create(outputFile)
			if err != nil {
				log.Fatalf("无法创建文件: %v", err)
			}
			defer file.Close()

			writer := csv.NewWriter(file)
			defer writer.Flush()
			writer.Write([]string{"bet", "win", "sp", "detail"})

			var wg sync.WaitGroup
			wg.Add(game.UserNum)

			writeChan := make(chan []string, 100)

			go func() {
				for row := range writeChan {
					if err := writer.Write(row); err != nil {
						log.Printf("写入失败: %v", err)
					}
				}
			}()

			for i := 0; i < game.UserNum; i++ {
				go func(id int) {
					defer wg.Done()
					spiderMan(i, writeChan, game.GameId, game.Spin, game.Cs, game.Ml, game.Fb, 10000)
				}(i)
			}

			// 等待任务完成
			wg.Wait()
			close(writeChan) // 关闭写入通道
			fmt.Println("🎮 Game ID: 普通数据采集完毕", game.GameId)
		}

		// 付费采集
		if game.FbStatus {
			outputFile := fmt.Sprintf("game_%d_fb.csv", game.GameId)
			file, err := os.Create(outputFile)
			if err != nil {
				log.Fatalf("无法创建文件: %v", err)
			}
			defer file.Close()

			writer := csv.NewWriter(file)
			defer writer.Flush()
			writer.Write([]string{"bet", "win", "sp", "detail"})

			var wg sync.WaitGroup
			wg.Add(game.FbUserNum)

			writeChan := make(chan []string, 100)

			go func() {
				for row := range writeChan {
					if err := writer.Write(row); err != nil {
						log.Printf("写入失败: %v", err)
					}
				}
			}()

			for i := 0; i < game.FbUserNum; i++ {
				go func(id int) {
					defer wg.Done()
					spiderMan(i, writeChan, game.GameId, game.Spin, game.Cs, game.Ml, game.Fb, 1000)
				}(i)
			}

			// 等待任务完成
			wg.Wait()
			close(writeChan) // 关闭写入通道
			fmt.Println("🎮 Game ID: 普通数据采集完毕", game.GameId)
		}
	}

	// 优先使用 CLI 参数（覆盖 YAML）
	// if *id != -1 {
	// 	cfg.ID = *id
	// }
	// if *genRtp != -1 {
	// 	cfg.GenRTP = (*genRtp == 1)
	// }

	// ✅ 使用配置
	// fmt.Println("🎮 Game ID:", cfg.ID)
	// fmt.Println("⚙️  是否生成 RTP:", cfg.GenRTP)

	// var fbLen int = 0
	// if cfg.Fb > 0 {
	// 	fbLen = 2
	// }
	// 不存在则开始采集

	// 读取yaml配置
	// 获取并发数量 maxUserNum

	// 获取指定数量的atk
	// wcfg, _ := loadConfig("run-cache.yaml")
	// atks := []string{}
	// for i := 0; i < cfg.MaxUserNum+fbLen; i++ {
	// 	atk, _ := getParamsFromFortuneSlot()
	// 	atks = append(atks, atk)
	// }

	// wcfg.AtkList = atks

	// 创建文件写入
	// writeYAML("run-cache.yaml", wcfg)

	// 启动指定数量任务的user开始采集 采集数量高于2万，并且rtp能够符合退出采集

	// outputFile := fmt.Sprintf("game_%d.csv", cfg.ID)
	// if !cfg.SpinAction {
	// 	outputFile += "_temp"
	// }
	// fbOutFile := fmt.Sprintf("game_%d_fb.csv", cfg.ID)
	// if cfg.Fb != 2 {
	// 	outputFile += "_temp"
	// }

	// 创建文件 & writer
	// file, err := os.Create(outputFile)
	// if err != nil {
	// 	log.Fatalf("无法创建文件: %v", err)
	// }
	// defer file.Close()

	// file1, err := os.Create(fbOutFile)
	// if err != nil {
	// 	log.Fatalf("无法创建文件: %v", err)
	// }
	// defer file1.Close()

	// writer := csv.NewWriter(file)
	// defer writer.Flush()

	// fbWriter := csv.NewWriter(file1)
	// defer fbWriter.Flush()

	// 写表头
	// writer.Write([]string{"bet", "win", "sp", "detail"})
	// fbWriter.Write([]string{"bet", "win", "sp", "detail"})

	// 写入队列（channel）
	// writeChan := make(chan []string, 100)
	// var wg sync.WaitGroup

	// // 启动 writer goroutine（唯一写入者）
	// go func() {
	// 	for row := range writeChan {
	// 		if err := writer.Write(row); err != nil {
	// 			log.Printf("写入失败: %v", err)
	// 		}
	// 	}
	// }()

	// fbWriteChan := make(chan []string, 100)
	// go func() {
	// 	for row := range fbWriteChan {
	// 		if err := fbWriter.Write(row); err != nil {
	// 			log.Printf("写入失败: %v", err)
	// 		}
	// 	}
	// }()

	// 启动多个任务
	// wg.Add(len(atks) + fbLen)

	// for w := 0; w < len(atks); w++ {
	// 	// fmt.Printf("%d / %d / %d \n", len(atks), len(atks)+fbLen, w)
	// 	go func(id int, atk string) {
	// 		defer wg.Done()
	// 		if !cfg.SpinAction && w < len(atks)-2 {
	// 			return
	// 		}
	// 		lastSid := "0"

	// 		// 请求循环
	// 		// 初始化 Colly
	// 		c := colly.NewCollector(
	// 			colly.MaxDepth(1),
	// 		)

	// 		c.UserAgent = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"

	// 		// 设置 Headers
	// 		c.OnRequest(func(r *colly.Request) {
	// 			r.Headers.Set("Content-Type", "application/x-www-form-urlencoded")
	// 			r.Headers.Set("Origin", "https://m-pg2.fortureslot.com")
	// 			r.Headers.Set("Referer", "https://m-pg2.fortureslot.com/")
	// 			r.Headers.Set("Accept-Language", "zh-CN,zh;q=0.9")
	// 		})

	// 		detail := []json.RawMessage{}
	// 		sessionIndex := 0
	// 		sp := 0

	// 		// 处理响应
	// 		c.OnResponse(func(resp *colly.Response) {
	// 			var parsed SpinResponse
	// 			// var rawBuf bytes.Bufferw2q
	// 			var si Si

	// 			// 压缩原始 JSON
	// 			// if err := json.Compact(&rawBuf, resp.Body); err != nil {
	// 			// 	rawBuf.WriteString("{}")
	// 			// }

	// 			if err := json.Unmarshal(resp.Body, &parsed); err != nil {
	// 				log.Println("解析失败:", err)
	// 				return
	// 			}

	// 			// fmt.Printf("原始响应: %+v \n", string(parsed.Dt.Si))

	// 			if err := json.Unmarshal(parsed.Dt.Si, &si); err != nil {
	// 				panic(err)
	// 			}

	// 			// 记录 raw json
	// 			detail = append(detail, json.RawMessage(parsed.Dt.Si))
	// 			lastSid = si.Sid

	// 			fmt.Printf("Group %d - Round %d - SID: %s - Balance: %2.f - ST: %d\n", w, sessionIndex, lastSid, si.Bl, si.St)

	// 			if si.Fs != nil && strings.TrimSpace(string(si.Fs)) != "null" {
	// 				// fmt.Println("Raw detail:", string(si.Fs))
	// 				sp = 1
	// 			}

	// 			// 检查 session 结束
	// 			if si.Nst == 1 {
	// 				detailJson, err := json.Marshal(detail)
	// 				if err != nil {
	// 					detailJson = []byte("[]")
	// 				}
	// 				// writer.Write([]string{
	// 				// 	fmt.Sprintf("%d", sessionIndex),
	// 				// 	string(detailJson),
	// 				// })
	// 				// writer.Flush()

	// 				sessionIndex++

	// 				if cfg.Fb > 0 && sessionIndex%500 == 0 {
	// 					atkTemp, _ := getParamsFromFortuneSlot()
	// 					atk = atkTemp
	// 				}

	// 				// fmt.Printf("aw %f \n", si.Aw)
	// 				row := []string{fmt.Sprintf("%f", si.Tbb), fmt.Sprintf("%f", si.Aw), strconv.Itoa(sp), string(detailJson)}

	// 				if cfg.Fb > 0 && w >= len(atks)-2 {
	// 					fbWriteChan <- row
	// 				} else {
	// 					writeChan <- row
	// 				}

	// 				detail = nil // 清空准备下一轮
	// 				sp = 0
	// 			}
	// 		})

	// 		for sessionIndex < 10000 {
	// 			traceId := uuid.New().String()

	// 			formBody := fmt.Sprintf("cs=%f&ml=%d&sn=1&id=%s&wk=0_C&btt=1&atk=%s&pf=4", cfg.Cs, cfg.Ml, lastSid, atk)
	// 			if cfg.Fb > 0 && w >= len(atks)-2 {
	// 				formBody = formBody + fmt.Sprintf("&fb=%d", cfg.Fb)
	// 			}

	// 			url := fmt.Sprintf("https://api.fortureslot.com/game-api/%d/v2/%s?traceId=%s", cfg.ID, cfg.Spin, traceId)

	// 			err := c.PostRaw(url, []byte(formBody))
	// 			if err != nil {
	// 				log.Println("请求失败:", err)
	// 			}

	// 			time.Sleep(50 * time.Millisecond)
	// 		}

	// 		// rows := generateData(id, rowsPerWorker)
	// 		// for _, row := range rows {
	// 		// writeChan <- row // 推入写入通道
	// 		// }
	// 	}(w, atks[w])
	// }

	// 等待任务完成
	// wg.Wait()
	// close(writeChan) // 关闭写入通道
}

func spiderMan(group int, writeChan chan []string, gameId int, spin string, cs float32, ml int, fb int, num int) {

	lastSid := "0"
	atk, _ := getParamsFromFortuneSlot()
	// fmt.Printf("atk123: %s\n", atk)

	// 请求循环
	// 初始化 Colly
	c := colly.NewCollector(
		colly.MaxDepth(1),
	)

	c.UserAgent = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"

	// 设置 Headers
	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Headers.Set("Origin", "https://m-pg2.fortureslot.com")
		r.Headers.Set("Referer", "https://m-pg2.fortureslot.com/")
		r.Headers.Set("Accept-Language", "zh-CN,zh;q=0.9")
	})

	detail := []json.RawMessage{}
	sessionIndex := 0
	sp := 0

	// 处理响应
	c.OnResponse(func(resp *colly.Response) {
		var parsed SpinResponse
		// var rawBuf bytes.Bufferw2q
		var si Si

		// 压缩原始 JSON
		// if err := json.Compact(&rawBuf, resp.Body); err != nil {
		// 	rawBuf.WriteString("{}")
		// }

		if err := json.Unmarshal(resp.Body, &parsed); err != nil {
			log.Println("解析失败:", err)
			return
		}

		// fmt.Printf("原始响应error: %+v \n", string(parsed.Err))
		// fmt.Printf("原始响应: %+v \n", string(parsed.Dt.Si))

		if err := json.Unmarshal(parsed.Dt.Si, &si); err != nil {
			fmt.Printf("原始响应error: %+v \n", string(parsed.Err))
			fmt.Printf("原始响应: %+v \n", string(parsed.Dt.Si))
			detail = nil
			lastSid = "0"
			atk, _ = getParamsFromFortuneSlot()
			return
			// panic(err)
		}

		// 记录 raw json
		detail = append(detail, json.RawMessage(parsed.Dt.Si))
		lastSid = si.Sid

		// fmt.Printf("Group %d - Round %d - SID: %s - Balance: %2.f - ST: %d\n", w, sessionIndex, lastSid, si.Bl, si.St)

		if si.Fs != nil && strings.TrimSpace(string(si.Fs)) != "null" {
			sp = 1
		}

		// 检查 session 结束
		if si.Nst == 1 {
			detailJson, err := json.Marshal(detail)
			if err != nil {
				detailJson = []byte("[]")
			}
			// writer.Write([]string{
			// 	fmt.Sprintf("%d", sessionIndex),
			// 	string(detailJson),
			// })
			// writer.Flush()

			sessionIndex++
			// if sessionIndex%10 == 0 {
			fmt.Printf("Group %d - Round %d - SID: %s - Balance: %2.f - ST: %d\n", group, sessionIndex, lastSid, si.Bl, si.St)
			// }

			// if cfg.Fb > 0 && sessionIndex%500 == 0 {
			// 	atkTemp, _ := getParamsFromFortuneSlot()
			// 	atk = atkTemp
			// }

			// fmt.Printf("aw %f \n", si.Aw)
			row := []string{fmt.Sprintf("%f", si.Tbb), fmt.Sprintf("%f", si.Aw), strconv.Itoa(sp), string(detailJson)}

			// if cfg.Fb > 0 && w >= len(atks)-2 {
			// 	fbWriteChan <- row
			// } else {
			writeChan <- row
			// }

			detail = nil // 清空准备下一轮
			sp = 0
			if si.Bl < 2000 {
				fmt.Printf("余额过低，获取新atk: %f\n", si.Bl)
				lastSid = "0"
				atk, _ = getParamsFromFortuneSlot()
			}
		}
	})

	for sessionIndex < num {
		traceId := uuid.New().String()

		formBody := fmt.Sprintf("cs=%.2f&ml=%d&sn=1&id=%s&wk=0_C&btt=1&atk=%s&pf=4", cs, ml, lastSid, atk)
		if fb > 0 {
			formBody = formBody + fmt.Sprintf("&fb=%d", fb)
		}

		// fmt.Printf("formBody: %s\n", formBody)

		url := fmt.Sprintf("https://api.fortureslot.com/game-api/%d/v2/%s?traceId=%s", gameId, spin, traceId)

		err := c.PostRaw(url, []byte(formBody))
		if err != nil {
			log.Println("请求失败:", err)
		}

		time.Sleep(50 * time.Millisecond)
	}

	// rows := generateData(id, rowsPerWorker)
	// for _, row := range rows {
	// writeChan <- row // 推入写入通道
	// }
}

// func loadConfig(path string) (*RunCache, error) {
// 	file, err := os.Open(path)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	var cfg RunCache
// 	if err := yaml.NewDecoder(file).Decode(&cfg); err != nil {
// 		return nil, err
// 	}
// 	return &cfg, nil
// }

// func writeYAML(path string, cfg *RunCache) error {
// 	file, err := os.Create(path)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()

// 	encoder := yaml.NewEncoder(file)
// 	defer encoder.Close()
// 	return encoder.Encode(cfg)
// }

// func readLastSid(filepath string, defaultSid string) string {
// 	content, err := os.ReadFile(filepath)
// 	if err != nil {
// 		fmt.Println("⚠️ 无法读取 last_sid.txt，使用默认值:", defaultSid)
// 		return defaultSid
// 	}
// 	return strings.TrimSpace(string(content))
// }

// func writeLastSid(filepath string, sid string) error {
// 	return os.WriteFile(filepath, []byte(sid), 0644)
// }

type LaunchResponse struct {
	Data struct {
		Url string `json:"Url"`
	} `json:"data"`
}

func getParamsFromFortuneSlot() (string, error) {
	// 模拟 userID
	rand.Seed(time.Now().UnixNano())
	userID := fmt.Sprintf("%d", rand.Intn(100000)+700000)

	// 请求参数
	reqBody := `{"gameID":"pg_29","language":"en","userID":"` + userID + `"}`

	client := &http.Client{}
	// req, err := http.NewRequest("POST", "https://gamecenter.fortureslots.com/api/v1/game/launch", strings.NewReader(reqBody))
	req, err := http.NewRequest("POST", "https://gamecenter.fortureslot.com/api/v1/game/launch", strings.NewReader(reqBody))
	if err != nil {
		return "", err
	}

	// 设置 Header
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("appid", "faketrans")
	req.Header.Set("appsecret", "b6337af9-a91a-4085-b1f2-466923470735")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// 读取响应
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	// 解析 JSON
	var parsed LaunchResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		log.Println("解析 JSON 失败:", err)
		log.Println("原始响应:", string(body))
		return "", err
	}

	// 解析 URL 参数
	u, err := url.Parse(parsed.Data.Url)
	if err != nil {
		return "", err
	}
	q := u.Query()
	atk := q.Get("ops")
	log.Println("atk:", string(atk))

	return atk, nil
	// // 构造 payload
	// payload := map[string]interface{}{
	// 	"cs":  0.05,
	// 	"ml":  1,
	// 	"fb":  0,
	// 	"atk": atk,
	// }

	// payloadJSON, err := json.Marshal(payload)
	// if err != nil {
	// 	return "", err
	// }

	// return string(payloadJSON), nil
}
