package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type Config struct {
	BotId    string `json:"botId"`
	Lang     string `json:"bolangtId"`
	Login    string `json:"login"`
	Password string `json:"password"`
}

type UserManager struct {
	Client      *http.Client
	BotId       string
	Lang        string
	Login       string
	Password    string
	AccessToken string
}

type AuthRequest struct {
	Lang     string `json:"lang"`
	Login    string `json:"login"`
	Password string `json:"password"`
}

type AuthResponse struct {
	IsAuth      bool   `json:"isAuth"`
	AccessToken string `json:"accessToken"`
}

type MallingRequest struct {
	TelegramID string `json:"telegramId"`
	BotId      string `json:"botId"`
	Message    string `json:"messageText"`
}
type ClientsResponse struct {
	Count      int
	SingleType bool
	Rows       []Client
}

type Client struct {
	Id           int    `json:"id"`
	Caption      string `json:"caption"`
	Description  string `json:"description"`
	UserName     string `json:"userName"`
	IsGroup      bool   `json:"isGroup"`
	Status       int    `json:"status"`
	BotId        int    `json:"botId"`
	Block        bool   `json:"block"`
	MessageCount string `json:"messageCount"`
	Max          int    `json:"max"`
	Coin         int    `json:"coin"`
	CountRef     int    `json:"countRef"`
	IsTelegram   bool   `json:"isTelegram"`
	IsVK         bool   `json:"isVK"`
	IsFb         bool   `json:"isFb"`
	VkId         string `json:"vkId"`
}

type ClientInfoResponse struct {
	ID         int    `json:"id"`
	TelegramID string `json:"telegramId"`
	UserName   string `json:"userName"`
}

func loadConfig(filename string) (*Config, error) {

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия файла конфигурации: %v", err)
	}
	defer file.Close()
	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения файла конфигурации: %v", err)
	}
	var config Config
	if err := json.Unmarshal(bytes, &config); err != nil {
		return nil, fmt.Errorf("ошибка енкодинга json: %v", err)
	}
	return &config, nil
}

func (u *UserManager) GetAccessToken() error {
	log.Println("Запрос токена доступа")
	url := "https://v2.whitehaze.ru/api/auth"
	data := AuthRequest{
		Lang:     u.Lang,
		Login:    u.Login,
		Password: u.Password,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("ошибка при создании запроса: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := u.Client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка при отправке запроса: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("ошибка при чтении ответа: %v", err)
	}

	log.Printf("Статус ответа: %v", resp.StatusCode)
	var authResponse AuthResponse

	json.Unmarshal(body, &authResponse)
	u.AccessToken = authResponse.AccessToken
	log.Printf("Ответ: %v", authResponse.AccessToken)
	return nil
}

func (u *UserManager) Malling(telegramID string, message string) error {
	fmt.Printf("Отправляю сообщение %v", telegramID)
	url := "https://v2.whitehaze.ru/api/clients/message"
	data := MallingRequest{
		TelegramID: telegramID,
		BotId:      u.BotId,
		Message:    message,
	}
	dataJson, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("ошибка при формировании json: %v", err)
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(dataJson))
	if err != nil {
		return fmt.Errorf("ошибка при формировании запроса: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	authToken := fmt.Sprintf("JWT %v", u.AccessToken)
	req.Header.Set("Authorization", authToken)

	resp, err := u.Client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка при отправке запроса: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("ошибка при декодировании запроса: %v", err)
	}

	log.Printf("Статус ответа: %d", resp.StatusCode)
	log.Printf("Ответ сервера: %s", string(body))

	// Проверяем статус ответа
	if resp.StatusCode != 200 {
		return fmt.Errorf("ошибка API: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (u *UserManager) GetClients() ([]Client, error) {
	fmt.Printf("Получаю список клиентов")
	url := "https://v2.whitehaze.ru/api/clients?offset=0"
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка при формировании запроса: %v", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("JWT %v", u.AccessToken))

	resp, err := u.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка при получении клиентов: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	var clientsResponse ClientsResponse
	err = json.Unmarshal(body, &clientsResponse)
	if err != nil {
		return nil, fmt.Errorf("ошибка при декодировании ответа: %v", err)
	}
	var resultClients []Client
	for _, v := range clientsResponse.Rows {
		if v.Status != -1 && v.IsTelegram {
			resultClients = append(resultClients, v)
		}
	}

	log.Printf("Отфильтровано клиентов: %d из %d", len(resultClients), len(clientsResponse.Rows))
	return resultClients, nil
}

func (u *UserManager) GetClientInfo(userId int) (*ClientInfoResponse, error) {
	url := fmt.Sprintf("https://v2.whitehaze.ru/api/clients/info?id=%v", userId)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка при формировании запроса: %v", err)
	}
	authToken := fmt.Sprintf("JWT %v", u.AccessToken)
	req.Header.Set("Authorization", authToken)

	resp, err := u.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка при отправке запроса: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ошибка при декодировании запроса: %v", err)
	}
	var clientInfo ClientInfoResponse
	err = json.Unmarshal(body, &clientInfo)
	if err != nil {
		return nil, fmt.Errorf("ошибка при парсинге json: %v", err)
	}
	return &clientInfo, nil
}

func (u *UserManager) GetClientInfoBatch(clients []Client, workers int) ([]ClientInfoResponse, error) {
	log.Printf("Получение информации о %d клиентах с %d воркерами", len(clients), workers)
	clientChan := make(chan Client, len(clients))
	resultChan := make(chan ClientInfoResponse, len(clients))
	errorChan := make(chan error, len(clients))

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for client := range clientChan {
				log.Printf("Воркер %d обрабатывает клиента %d", workerID, client.Id)

				clientInfo, err := u.GetClientInfo(client.Id)
				if err != nil {
					log.Printf("Ошибка получения информации о клиенте %d: %v", client.Id, err)
					errorChan <- err
					continue
				}
				resultChan <- *clientInfo

				time.Sleep(100 * time.Millisecond)
			}
		}(i)

	}

	go func() {
		for _, client := range clients {
			clientChan <- client
		}
		close(clientChan)
	}()

	// Ждем завершения всех воркеров
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	var results []ClientInfoResponse
	for result := range resultChan {
		results = append(results, result)
	}
	log.Printf("Обработано: %d клиентов", len(results))
	return results, nil

}

func (u *UserManager) SaveClientInfoToJSON(clientInfos []ClientInfoResponse, filename string) error {
	log.Printf("Сохранение информации о %d клиентах в файл %s", len(clientInfos), filename)

	jsonData, err := json.MarshalIndent(clientInfos, "", "    ")
	if err != nil {
		return fmt.Errorf("ошибка при формировании JSON: %v", err)
	}

	if err := os.WriteFile(filename, jsonData, 0644); err != nil {
		return fmt.Errorf("ошибка при записи JSON в файл: %v", err)
	}

	log.Printf("Информация о клиентах успешно сохранена в %s", filename)
	return nil
}

func (u *UserManager) SaveClientsToJSON(clients []Client, filename string) error {
	log.Printf("Сохранение %d клиентов в файл %s", len(clients), filename)
	jsonData, err := json.MarshalIndent(clients, "", "    ")
	if err != nil {
		return fmt.Errorf("ошибка при формировании JSON: %v", err)
	}
	if err := os.WriteFile(filename, jsonData, 0644); err != nil {
		return fmt.Errorf("ошибка при записи JSON в файл: %v", err)
	}
	log.Printf("Клиенты успешно сохранены в %s", filename)
	return nil
}

func (u *UserManager) TestMalling(message string) error {
	return u.Malling("796508261", message) // Возвращаем ошибку вместо игнорирования
}

func (u *UserManager) MassMalling(workers int, users []ClientInfoResponse, message string, start_index int) error {
	log.Printf("Начинаю массовую рассылку с %d воркерами", workers)
	userChan := make(chan ClientInfoResponse, len(users))
	errorChan := make(chan error, len(users))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerId int) {
			defer wg.Done()
			for user := range userChan {
				log.Printf("Воркер %d обрабатывает пользователя %d, с телеграмм id %v", workerId, user.ID, user.TelegramID)
				err := u.Malling(user.TelegramID, message)
				if err != nil {
					log.Printf("Ошибка при отправке сообщения пользователю %d: %v", user.ID, err)
					errorChan <- err // Добавить ошибку в канал
				}
			}
		}(i)
	}

	go func() {
		for i := start_index; i < len(users); i++ {
			userChan <- users[i]
		}
		close(userChan)
	}()

	go func() {
		wg.Wait()
		close(errorChan)
	}()
	for err := range errorChan {
		log.Printf("Ошибка при массовой рассылке: %v", err)
	}
	log.Printf("Массовая рассылка завершена")
	return nil
}

func main() {
	logFile, err := os.OpenFile("logs.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Не удалось открыть файл для логов: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	httpClient := http.Client{}

	config, err := loadConfig("config.json")
	if err != nil {
		log.Printf("Ошибка загркзки конфига: %v", err)
	}

	userManager := UserManager{
		Client:   &httpClient,
		BotId:    config.BotId,
		Lang:     config.Lang,
		Login:    config.Login,
		Password: config.Password,
	}
	message := `<b>ВНИМАНИЕ</b>❗️
	<b>ВНИМАНИЕ</b>`

	// Проверяем успешность получения токена
	if err := userManager.GetAccessToken(); err != nil {
		log.Printf("Ошибка получения токена: %v", err)
		return
	}

	// Проверяем, что токен получен
	if userManager.AccessToken == "" {
		log.Printf("Токен не получен")
		return
	}

	// Отправляем сообщение с обработкой ошибки
	if err := userManager.TestMalling(message); err != nil {
		log.Printf("Ошибка отправки сообщения: %v", err)
		return
	}

	log.Println("Сообщение успешно отправлено")

	clients, err := userManager.GetClients()
	if err != nil {
		log.Printf("Ошибка получения пользователей: %v", err)
	}

	err = userManager.SaveClientsToJSON(clients, "users_json/all_clients.json")
	if err != nil {
		log.Printf("Ошибка сохранения клиентов: %v", err)
		return
	}

	log.Printf("Начинаем параллельное получение информации о %d клиентах", len(clients))
	start := time.Now()

	// Получаем информацию о клиентах параллельно (50 воркеров)
	clientInfos, err := userManager.GetClientInfoBatch(clients, 300)
	if err != nil {
		log.Printf("Ошибка получения информации о клиентах: %v", err)
		return
	}

	duration := time.Since(start)
	log.Printf("Получение информации о клиентах заняло: %v", duration)

	// Сохраняем информацию о клиентах
	err = userManager.SaveClientInfoToJSON(clientInfos, "users_json/client_info.json")
	if err != nil {
		log.Printf("Ошибка сохранения информации о клиентах: %v", err)
		return
	}

	// Выводим первые 10 результатов для примера
	for i, clientInfo := range clientInfos {
		if i >= 10 {
			log.Printf("... и еще %d клиентов", len(clientInfos)-10)
			break
		}
		fmt.Printf("Пользователь - %v, TelegramID - %v\n", clientInfo.ID, clientInfo.TelegramID)
	}

	// err = userManager.MassMalling(10, clientInfos, message, 0)
	// if err != nil {
	// 	log.Printf("Ошибка массовой рассылки: %v", err)
	// 	return
	// }

	log.Printf("Обработка завершена. Всего клиентов: %d", len(clientInfos))
}
