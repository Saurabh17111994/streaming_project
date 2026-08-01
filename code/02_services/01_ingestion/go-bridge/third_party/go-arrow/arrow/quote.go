// quotes.go
package arrow

import (
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog/log"
)

// InfoQuoteMode is the URL segment for REST /info/quote/{mode} and /info/quotes/{mode}.
// This matches pyarrow_client.constants.QuoteMode (ltp, full, ohlcv). It is not the same
// as WebSocket StreamMode (which includes ltpc, quote, full, ltp for binary ticks).
type InfoQuoteMode string

const (
	InfoQuoteLTP   InfoQuoteMode = "ltp"
	InfoQuoteFull  InfoQuoteMode = "full"
	InfoQuoteOHLCV InfoQuoteMode = "ohlcv"
)

// QuoteInstrument is the JSON body shape for /info/quotes/{mode} (symbol + exchange only).
type QuoteInstrument struct {
	Exchange string `json:"exchange"`
	Symbol   string `json:"symbol"`
}

// QuoteRequest is kept for documentation compatibility; do not set Mode — it is omitted from JSON.
type QuoteRequest struct {
	Exchange string `json:"exchange"`
	Symbol   string `json:"symbol"`
	Mode     string `json:"mode,omitempty"`
}

// QuoteLTP is a common subset of quote fields when the API returns token/LTP/close style data.
type QuoteLTP struct {
	Token int `json:"token"`
	Ltp   int `json:"ltp"`
	Close int `json:"close"`
}

// QuoteLTPResponse is the batch quotes API envelope when data is a list of QuoteLTP-like objects.
type QuoteLTPResponse struct {
	Data   []QuoteLTP `json:"data"`
	Status string     `json:"status"`
}

// GetQuotes posts to /info/quotes/{mode} with a JSON array of {exchange, symbol} (no mode in body).
func (c *Client) GetQuotes(instruments []QuoteInstrument, mode InfoQuoteMode) ([]map[string]any, error) {
	endpoint := fmt.Sprintf("/info/quotes/%s", mode)

	payload, err := json.Marshal(instruments)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal quote requests")
		return nil, err
	}

	resp, err := c.request(endpoint, "POST", payload)
	if err != nil {
		log.Error().Err(err).Msg("Failed to fetch quotes")
		return nil, err
	}

	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Status string          `json:"status"`
	}
	if err := json.Unmarshal(resp, &envelope); err != nil {
		log.Error().Err(err).Msg("Failed to parse quotes response")
		return nil, err
	}
	if envelope.Status != "success" {
		return nil, fmt.Errorf("quotes retrieval failed with status: %s", envelope.Status)
	}

	var asSlice []map[string]any
	if err := json.Unmarshal(envelope.Data, &asSlice); err == nil {
		c.debugf("Quotes retrieved successfully", nil)
		return asSlice, nil
	}
	var one map[string]any
	if err := json.Unmarshal(envelope.Data, &one); err == nil && len(one) > 0 {
		return []map[string]any{one}, nil
	}
	return nil, fmt.Errorf("quotes data: unsupported JSON shape")
}

// GetQuote posts to /info/quote/{mode} with {"symbol","exchange"}.
func (c *Client) GetQuote(exchange Exchange, symbol string, mode InfoQuoteMode) (map[string]any, error) {
	endpoint := fmt.Sprintf("/info/quote/%s", mode)
	body := QuoteInstrument{Exchange: string(exchange), Symbol: symbol}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, err := c.request(endpoint, "POST", payload)
	if err != nil {
		log.Error().Err(err).Msg("Failed to fetch quote")
		return nil, err
	}
	var result struct {
		Data   map[string]any `json:"data"`
		Status string         `json:"status"`
	}
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	if result.Status != "success" {
		return nil, fmt.Errorf("quote retrieval failed with status: %s", result.Status)
	}
	return result.Data, nil
}
