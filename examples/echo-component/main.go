package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/furconz/ed-iot-device-sdk-go/greengrassipc"
)

// Component configuration structure
type Config struct {
	InputTopic  string  `json:"inputTopic"`
	OutputTopic string  `json:"outputTopic"`
	QOS         string  `json:"qos"`
	Prefix      string  `json:"prefix"`
	LogInterval int     `json:"logIntervalSeconds"`
}

// Default configuration
var defaultConfig = Config{
	InputTopic:  "echo/input",
	OutputTopic: "echo/output",
	QOS:         "AT_LEAST_ONCE",
	Prefix:      "[Echo]",
	LogInterval: 60,
}

// Component state
type Component struct {
	client       *greengrassipc.Client
	config       Config
	metrics      *Metrics
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// Metrics tracking
type Metrics struct {
	mu              sync.Mutex
	messagesReceived int64
	messagesSent     int64
	errors           int64
	lastReportTime   time.Time
}

func main() {
	log.Println("Starting Echo Component...")

	// Create component
	comp, err := NewComponent()
	if err != nil {
		log.Fatalf("Failed to create component: %v", err)
	}
	defer comp.Shutdown()

	// Report component as running
	if err := comp.ReportRunning(); err != nil {
		log.Printf("Warning: Failed to report running state: %v", err)
	}

	// Start component operations
	if err := comp.Run(); err != nil {
		log.Fatalf("Component error: %v", err)
	}
}

// NewComponent creates a new component instance
func NewComponent() (*Component, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create IPC client (auto-detects from environment)
	client, err := greengrassipc.NewClient(ctx, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create IPC client: %w", err)
	}

	comp := &Component{
		client: client,
		config: defaultConfig,
		metrics: &Metrics{
			lastReportTime: time.Now(),
		},
		ctx:    ctx,
		cancel: cancel,
	}

	// Load configuration
	if err := comp.LoadConfiguration(); err != nil {
		log.Printf("Warning: Failed to load configuration, using defaults: %v", err)
	} else {
		log.Printf("Loaded configuration: %+v", comp.config)
	}

	return comp, nil
}

// LoadConfiguration loads component configuration from Greengrass
func (c *Component) LoadConfiguration() error {
	resp, err := c.client.GetConfiguration(c.ctx, &greengrassipc.GetConfigurationRequest{})
	if err != nil {
		return err
	}

	// Parse configuration
	if resp.Value != nil {
		configJSON, err := json.Marshal(resp.Value)
		if err != nil {
			return fmt.Errorf("failed to marshal config: %w", err)
		}

		if err := json.Unmarshal(configJSON, &c.config); err != nil {
			return fmt.Errorf("failed to unmarshal config: %w", err)
		}
	}

	return nil
}

// ReportRunning reports the component as running to Greengrass
func (c *Component) ReportRunning() error {
	_, err := c.client.UpdateState(c.ctx, &greengrassipc.UpdateStateRequest{
		State: greengrassipc.ReportedLifecycleStateRunning,
	})
	if err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}
	log.Println("Reported component state: RUNNING")
	return nil
}

// Run starts the component's main operations
func (c *Component) Run() error {
	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start subscription to input topic
	c.wg.Add(1)
	go c.subscribeToInput()

	// Start metrics reporting
	c.wg.Add(1)
	go c.reportMetrics()

	// Start configuration update listener
	c.wg.Add(1)
	go c.listenForConfigUpdates()

	// Wait for shutdown signal
	select {
	case sig := <-sigChan:
		log.Printf("Received signal: %v, shutting down...", sig)
	case <-c.ctx.Done():
		log.Println("Context cancelled, shutting down...")
	}

	return nil
}

// subscribeToInput subscribes to the input topic and processes messages
func (c *Component) subscribeToInput() {
	defer c.wg.Done()

	log.Printf("Subscribing to IoT Core topic: %s", c.config.InputTopic)

	// Convert QOS string to type
	qos := greengrassipc.QOSAtLeastOnce
	if c.config.QOS == "AT_MOST_ONCE" {
		qos = greengrassipc.QOSAtMostOnce
	}

	// Subscribe to IoT Core topic
	sub, err := c.client.SubscribeToIoTCore(c.ctx, &greengrassipc.SubscribeToIoTCoreRequest{
		TopicName: c.config.InputTopic,
		QOS:       qos,
	})
	if err != nil {
		log.Printf("ERROR: Failed to subscribe: %v", err)
		c.recordError()
		return
	}
	defer sub.Close()

	log.Printf("Successfully subscribed to %s", c.config.InputTopic)

	// Process messages
	for {
		select {
		case <-c.ctx.Done():
			log.Println("Stopping input subscription...")
			return

		case msg := <-sub.Messages():
			if msg.Message == nil {
				continue
			}
			c.handleMessage(msg.Message)

		case err := <-sub.Errors():
			log.Printf("Subscription error: %v", err)
			c.recordError()
		}
	}
}

// handleMessage processes an incoming message and sends a response
func (c *Component) handleMessage(msg *greengrassipc.MQTTMessage) {
	c.recordMessageReceived()

	log.Printf("Received message on %s: %s", msg.TopicName, string(msg.Payload))

	// Create response message
	response := fmt.Sprintf("%s %s", c.config.Prefix, string(msg.Payload))

	// Publish response to output topic
	qos := greengrassipc.QOSAtLeastOnce
	if c.config.QOS == "AT_MOST_ONCE" {
		qos = greengrassipc.QOSAtMostOnce
	}

	_, err := c.client.PublishToIoTCore(c.ctx, &greengrassipc.PublishToIoTCoreRequest{
		TopicName: c.config.OutputTopic,
		QOS:       qos,
		Payload:   []byte(response),
	})
	if err != nil {
		log.Printf("ERROR: Failed to publish response: %v", err)
		c.recordError()
		return
	}

	c.recordMessageSent()
	log.Printf("Published response to %s: %s", c.config.OutputTopic, response)
}

// reportMetrics periodically reports metrics to CloudWatch
func (c *Component) reportMetrics() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(c.config.LogInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			log.Println("Stopping metrics reporting...")
			// Report final metrics
			c.publishMetrics()
			return

		case <-ticker.C:
			c.publishMetrics()
		}
	}
}

// publishMetrics publishes current metrics to CloudWatch
func (c *Component) publishMetrics() {
	c.metrics.mu.Lock()
	received := c.metrics.messagesReceived
	sent := c.metrics.messagesSent
	errors := c.metrics.errors
	timeSinceLastReport := time.Since(c.metrics.lastReportTime).Seconds()
	c.metrics.lastReportTime = time.Now()
	c.metrics.mu.Unlock()

	// Calculate rates
	receiveRate := float64(received) / timeSinceLastReport
	sendRate := float64(sent) / timeSinceLastReport
	errorRate := float64(errors) / timeSinceLastReport

	// Log metrics
	log.Printf("METRICS: Received=%d (%.2f/s), Sent=%d (%.2f/s), Errors=%d (%.2f/s)",
		received, receiveRate, sent, sendRate, errors, errorRate)

	// Publish to CloudWatch via Greengrass
	now := time.Now()
	metrics := []greengrassipc.Metric{
		{
			Name:      "MessagesReceived",
			Unit:      greengrassipc.MetricUnitTypeCount,
			Value:     float64(received),
			Timestamp: &now,
		},
		{
			Name:      "MessagesSent",
			Unit:      greengrassipc.MetricUnitTypeCount,
			Value:     float64(sent),
			Timestamp: &now,
		},
		{
			Name:      "Errors",
			Unit:      greengrassipc.MetricUnitTypeCount,
			Value:     float64(errors),
			Timestamp: &now,
		},
		{
			Name:      "ReceiveRate",
			Unit:      greengrassipc.MetricUnitTypeCountPerSecond,
			Value:     receiveRate,
			Timestamp: &now,
		},
	}

	_, err := c.client.PutComponentMetric(c.ctx, &greengrassipc.PutComponentMetricRequest{
		Metrics: metrics,
	})
	if err != nil {
		log.Printf("Warning: Failed to publish metrics: %v", err)
	}
}

// listenForConfigUpdates listens for configuration changes
func (c *Component) listenForConfigUpdates() {
	defer c.wg.Done()

	log.Println("Listening for configuration updates...")

	sub, err := c.client.SubscribeToConfigurationUpdate(c.ctx,
		&greengrassipc.SubscribeToConfigurationUpdateRequest{})
	if err != nil {
		log.Printf("Warning: Failed to subscribe to config updates: %v", err)
		return
	}
	defer sub.Close()

	for {
		select {
		case <-c.ctx.Done():
			log.Println("Stopping configuration listener...")
			return

		case event := <-sub.Messages():
			if event.ConfigurationUpdateEvent != nil {
				log.Println("Configuration updated, reloading...")
				if err := c.LoadConfiguration(); err != nil {
					log.Printf("ERROR: Failed to reload configuration: %v", err)
				} else {
					log.Printf("New configuration: %+v", c.config)
				}
			}

		case err := <-sub.Errors():
			log.Printf("Config subscription error: %v", err)
		}
	}
}

// Metrics tracking methods
func (c *Component) recordMessageReceived() {
	c.metrics.mu.Lock()
	c.metrics.messagesReceived++
	c.metrics.mu.Unlock()
}

func (c *Component) recordMessageSent() {
	c.metrics.mu.Lock()
	c.metrics.messagesSent++
	c.metrics.mu.Unlock()
}

func (c *Component) recordError() {
	c.metrics.mu.Lock()
	c.metrics.errors++
	c.metrics.mu.Unlock()
}

// Shutdown gracefully shuts down the component
func (c *Component) Shutdown() {
	log.Println("Shutting down component...")

	// Cancel context to stop all goroutines
	c.cancel()

	// Wait for all goroutines to finish
	c.wg.Wait()

	// Close IPC client
	if c.client != nil {
		c.client.Close()
	}

	log.Println("Component shutdown complete")
}
