package vetesclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"

	"github.com/GBA-BI/tes-k8s-agent/pkg/vetesclient/models"
)

const (
	ga4ghAPIPrefix = "/api/ga4gh/tes/v1"
	otherAPIPrefix = "/api/v1"
)

var (
	// ErrNotFound ...
	ErrNotFound = errors.New("not found")
	// ErrBadRequest ...
	ErrBadRequest = errors.New("bad request")
)

// Client ...
type Client interface {
	ListTasks(ctx context.Context, req *models.ListTasksRequest) (*models.ListTasksResponse, error)
	GetTask(ctx context.Context, req *models.GetTaskRequest) (*models.GetTaskResponse, error)
	UpdateTask(ctx context.Context, req *models.UpdateTaskRequest) (*models.UpdateTaskResponse, error)

	PutCluster(ctx context.Context, req *models.PutClusterRequest) (*models.PutClusterResponse, error)
}

type impl struct {
	endpoint string
	cli      *http.Client
}

// NewClient ...
func NewClient(opts *Options) Client {
	cli := &http.Client{Timeout: opts.Timeout}
	return &impl{endpoint: opts.Endpoint, cli: cli}
}

var _ Client = (*impl)(nil)

// ListTasks ...
func (i *impl) ListTasks(ctx context.Context, req *models.ListTasksRequest) (*models.ListTasksResponse, error) {
	resp := new(models.ListTasksResponse)
	if err := i.doRequest(ctx, http.MethodGet, fmt.Sprintf("%s%s/tasks", i.endpoint, ga4ghAPIPrefix), req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GetTask ...
func (i *impl) GetTask(ctx context.Context, req *models.GetTaskRequest) (*models.GetTaskResponse, error) {
	resp := new(models.GetTaskResponse)
	if err := i.doRequest(ctx, http.MethodGet, fmt.Sprintf("%s%s/tasks/%s", i.endpoint, ga4ghAPIPrefix, req.ID), req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// UpdateTask ...
func (i *impl) UpdateTask(ctx context.Context, req *models.UpdateTaskRequest) (*models.UpdateTaskResponse, error) {
	resp := new(models.UpdateTaskResponse)
	if err := i.doRequest(ctx, http.MethodPatch, fmt.Sprintf("%s%s/tasks/%s", i.endpoint, otherAPIPrefix, req.ID), req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// PutCluster ...
func (i *impl) PutCluster(ctx context.Context, req *models.PutClusterRequest) (*models.PutClusterResponse, error) {
	resp := new(models.PutClusterResponse)
	if err := i.doRequest(ctx, http.MethodPut, fmt.Sprintf("%s%s/clusters/%s", i.endpoint, otherAPIPrefix, req.ID), req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (i *impl) doRequest(ctx context.Context, method, url string, req, resp interface{}) error {
	request, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return err
	}
	request.Header.Add("Accept", "application/json")

	query, err := parseQuery(req)
	if err != nil {
		return err
	}
	mergeQuery(request, query)

	if method == http.MethodPatch || method == http.MethodPost || method == http.MethodPut {
		request.Header.Add("Content-Type", "application/json")
		content, err := json.Marshal(req)
		if err != nil {
			return err
		}
		request.Body = io.NopCloser(bytes.NewReader(content))
	}

	var response *http.Response
	response, err = i.cli.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode > 399 {
		message, _ := io.ReadAll(response.Body)
		switch response.StatusCode {
		case http.StatusBadRequest:
			return fmt.Errorf("%s: %w", message, ErrBadRequest)
		case http.StatusNotFound:
			return fmt.Errorf("%s: %w", message, ErrNotFound)
		default:
			return fmt.Errorf("%d: %s", response.StatusCode, message)
		}
	}

	decoder := json.NewDecoder(response.Body)
	if err = decoder.Decode(resp); err != nil {
		return err
	}
	return nil
}

func mergeQuery(request *http.Request, query url.Values) {
	if len(query) == 0 {
		return
	}
	q := request.URL.Query()
	for k, vs := range query {
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	request.URL.RawQuery = q.Encode()
}

func parseQuery(obj interface{}) (url.Values, error) {
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("invalid obj type: %s", v.Type().Kind())
	}

	res := make(url.Values, 0)
	for i := 0; i < v.NumField(); i++ {
		tag, ok := v.Type().Field(i).Tag.Lookup("query")
		if !ok {
			continue
		}
		values, err := parseFieldValue(v.Field(i))
		if err != nil {
			return nil, err
		}
		if len(values) > 0 {
			res[tag] = values
		}
	}
	return res, nil
}

func parseFieldValue(field reflect.Value) ([]string, error) {
	if field.IsZero() {
		return nil, nil
	}
	switch field.Kind() {
	case reflect.Slice, reflect.Array:
		if field.Len() == 0 {
			return nil, nil
		}
		res := make([]string, 0, field.Len())
		for i := 0; i < field.Len(); i++ {
			s, err := getValueString(field.Index(i))
			if err != nil {
				return nil, err
			}
			res = append(res, s)
		}
		return res, nil
	default:
		s, err := getValueString(field)
		if err != nil {
			return nil, err
		}
		return []string{s}, nil
	}
}

func getValueString(v reflect.Value) (string, error) {
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Bool:
		return strconv.FormatBool(v.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10), nil
	case reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64), nil
	case reflect.Float32:
		return strconv.FormatFloat(v.Float(), 'g', -1, 32), nil
	case reflect.String:
		return v.String(), nil
	default:
		return "", fmt.Errorf("unsupported type %s", v.Type().String())
	}
}
