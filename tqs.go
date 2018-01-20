package tqs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

type Queue struct {
	Endpoint string
	Name     string
	url      string
	token    string
}

type Message struct {
	queue        Queue
	BodyText     string    `json:"body"`
	BodyType     string    `json:"type"`
	CreateDate   time.Time `json:"create_date"`
	VisibleDate  time.Time `json:"visible_date"`
	ExpireDate   time.Time `json:"expire_date"`
	LeaseUUID    string    `json:"lease_uuid"`
	LeaseTimeout int       `json:"lease_timeout"`
	LeaseDate    time.Time `json:"lease_date"`
}

type LeaseNotFoundError struct {
	message Message
}

func (e *LeaseNotFoundError) Error() string {
	return fmt.Sprintf("Lease <%s> does not exist or expired", e.message.LeaseUUID)
}

func (m Message) Delete() error {
	client := http.Client{Timeout: time.Second * 2}

	url := fmt.Sprintf("%s/leases/%s", m.queue.url, m.LeaseUUID)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	if m.queue.token != "" {
		req.Header.Set("Authentication", "token "+m.queue.token)
	}

	req.Close = true

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		return &LeaseNotFoundError{message: m}
	}

	return nil
}

type QueueStatistics struct {
	Visible int `json:"visible"`
	Delayed int `json:"delayed"`
	Leased  int `json:"leased"`
}

type QueueNotFoundError struct {
	queue Queue
}

func (e *QueueNotFoundError) Error() string {
	return fmt.Sprintf("Queue <%s> does not exist", e.queue.Name)
}

type QueueEmptyError struct {
	queue Queue
}

func (e *QueueEmptyError) Error() string {
	return fmt.Sprintf("Queue <%s> has no messages available", e.queue.Name)
}

type QueueAlreadyExistsError struct {
	queue Queue
}

func (e *QueueAlreadyExistsError) Error() string {
	return fmt.Sprintf("Queue <%s> already exist", e.queue.Name)
}

type QueueHTTPError struct {
	queue      Queue
	StatusCode int
}

func (e *QueueHTTPError) Error() string {
	return fmt.Sprintf("Queue <%s> return HTTP Status <%d>", e.queue.Name, e.StatusCode)
}

func NewQueue(endpoint string, name string, token string) Queue {
	return Queue{
		Endpoint: endpoint,
		Name:     name,
		url:      fmt.Sprintf("%s/queues/%s", endpoint, name),
		token:    token,
	}
}

func (q Queue) Statistics() (QueueStatistics, error) {
	client := http.Client{Timeout: time.Second * 2}

	url := fmt.Sprintf("%s/statistics", q.url)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return QueueStatistics{}, err
	}

	if q.token != "" {
		req.Header.Set("Authentication", "token "+q.token)
	}

	res, err := client.Do(req)
	if err != nil {
		return QueueStatistics{}, err
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return QueueStatistics{}, &QueueNotFoundError{queue: q}
	}

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		return QueueStatistics{}, err
	}

	statistics := QueueStatistics{}
	if err := json.Unmarshal(body, &statistics); err != nil {
		return QueueStatistics{}, err
	}

	return statistics, nil
}

func (q Queue) Exists() (bool, error) {
	_, err := q.Statistics()
	if err != nil {
		if _, ok := err.(*QueueNotFoundError); ok {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

type CreateQueueRequest struct {
	Name string `json:"name"`
}

func (q Queue) Create() error {
	client := http.Client{Timeout: time.Second * 2}

	createRequest := CreateQueueRequest{Name: q.Name}
	data, err := json.Marshal(createRequest)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/queues", q.Endpoint)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	if q.token != "" {
		req.Header.Set("Authentication", "token "+q.token)
	}

	req.Close = true
	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode == 409 {
		return &QueueAlreadyExistsError{queue: q}
	}

	if res.StatusCode != 200 {
		return &QueueHTTPError{queue: q, StatusCode: res.StatusCode}
	}

	return nil
}

type GetMessagesResponse struct {
	Messages []Message `json:"messages"`
}

type GetOptions struct {
	Wait   time.Duration
	Delete bool
	Retry  bool
}

func (q Queue) GetValue(options *GetOptions, v interface{}) error {
	msg, err := q.Get(options)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(msg.BodyText), v)
}

func (q Queue) Get(options *GetOptions) (Message, error) {
	client := http.Client{}

	req, err := http.NewRequest(http.MethodGet, q.url, nil)
	if err != nil {
		return Message{}, err
	}

	if q.token != "" {
		req.Header.Set("Authentication", "token "+q.token)
	}

	req.Close = true

	if options != nil {
		q := req.URL.Query()
		if options.Delete {
			q.Set("delete", "true")
		}
		if options.Wait > 0 {
			q.Set("wait_time", strconv.Itoa(int(options.Wait)/1000000000))
		}
		req.URL.RawQuery = q.Encode()
	}

	res, err := client.Do(req)
	if err != nil {
		return Message{}, err
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return Message{}, &QueueNotFoundError{queue: q}
	}

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		return Message{}, err
	}

	response := GetMessagesResponse{}
	if err := json.Unmarshal(body, &response); err != nil {
		return Message{}, err
	}

	if len(response.Messages) == 1 {
		msg := response.Messages[0]
		msg.queue = q
		return msg, nil
	}

	return Message{}, &QueueEmptyError{queue: q}
}

type MessageBody struct {
	Text string `json:"body"`
	Type string `json:"type"`
}

type PutMessageRequest struct {
	Messages []MessageBody `json:"messages"`
}

func (q Queue) Put(bodyText, bodyType string) error {
	client := http.Client{}

	request := PutMessageRequest{
		Messages: []MessageBody{MessageBody{Text: bodyText, Type: bodyType}},
	}

	data, err := json.Marshal(request)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, q.url, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	if q.token != "" {
		req.Header.Set("Authentication", "token "+q.token)
	}

	req.Close = true
	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode == 404 {
		return &QueueNotFoundError{queue: q}
	}

	if res.StatusCode != 200 {
		return &QueueHTTPError{queue: q, StatusCode: res.StatusCode}
	}

	return nil
}
