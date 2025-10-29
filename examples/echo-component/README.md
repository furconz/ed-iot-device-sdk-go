# Echo Component Example

A complete example Greengrass v2 component written in Go that demonstrates key SDK features.

## Features Demonstrated

This example shows how to:

- ✅ **Create an IPC client** with auto-configuration from environment
- ✅ **Subscribe to IoT Core topics** and receive messages
- ✅ **Publish messages to IoT Core** in response
- ✅ **Report component lifecycle state** (RUNNING)
- ✅ **Load and use configuration** from Greengrass
- ✅ **Listen for configuration updates** and reload dynamically
- ✅ **Publish custom metrics** to CloudWatch
- ✅ **Handle graceful shutdown** with signal handling

## What It Does

The Echo Component:

1. Subscribes to an IoT Core topic (`echo/input` by default)
2. Receives messages from that topic
3. Prepends a configurable prefix to each message
4. Publishes the modified message to an output topic (`echo/output` by default)
5. Tracks and reports metrics (messages received, sent, errors, rates)
6. Handles configuration updates without restarting
7. Shuts down gracefully on SIGTERM/SIGINT

## Configuration

The component accepts the following configuration parameters:

```json
{
  "inputTopic": "echo/input",
  "outputTopic": "echo/output",
  "qos": "AT_LEAST_ONCE",
  "prefix": "[Echo]",
  "logIntervalSeconds": 60
}
```

- **inputTopic**: IoT Core topic to subscribe to
- **outputTopic**: IoT Core topic to publish responses to
- **qos**: Quality of Service level (`AT_MOST_ONCE` or `AT_LEAST_ONCE`)
- **prefix**: Text to prepend to echoed messages
- **logIntervalSeconds**: How often to log and publish metrics

## Building

```bash
cd examples/echo-component

# Build for Linux
GOOS=linux GOARCH=amd64 go build -o echo-component main.go

# Build for ARM (Raspberry Pi, etc.)
GOOS=linux GOARCH=arm64 go build -o echo-component main.go

# Build for Windows
GOOS=windows GOARCH=amd64 go build -o echo-component.exe main.go
```

## Deploying to Greengrass

### 1. Build the Component

```bash
go build -o echo-component main.go
```

### 2. Create Component Artifacts

Upload the binary to an S3 bucket:

```bash
aws s3 cp echo-component s3://my-component-bucket/com.example.EchoComponent/1.0.0/
```

### 3. Create Component

Update the recipe with your S3 bucket location and create the component:

```bash
aws greengrassv2 create-component-version \
    --inline-recipe fileb://recipe.yaml
```

### 4. Deploy Component

Create a deployment to your Greengrass core device:

```bash
aws greengrassv2 create-deployment \
    --target-arn "arn:aws:iot:region:account:thing/MyGreengrassCoreDevice" \
    --components '{
        "com.example.EchoComponent": {
            "componentVersion": "1.0.0",
            "configurationUpdate": {
                "merge": "{\"inputTopic\":\"echo/input\",\"outputTopic\":\"echo/output\"}"
            }
        }
    }'
```

## Testing

Once deployed, test the component using the AWS IoT MQTT test client:

1. Subscribe to the output topic: `echo/output`
2. Publish a message to the input topic: `echo/input`
   ```json
   {
     "message": "Hello Greengrass!"
   }
   ```
3. You should receive: `[Echo] {"message": "Hello Greengrass!"}`

Or use the AWS CLI:

```bash
# Publish a test message
aws iot-data publish \
    --topic "echo/input" \
    --payload '{"message": "Hello from CLI"}' \
    --cli-binary-format raw-in-base64-out

# Subscribe to responses (in another terminal)
aws iot-data publish \
    --topic "echo/output" \
    --payload '' \
    --cli-binary-format raw-in-base64-out
```

## Viewing Logs

Check component logs on the Greengrass device:

```bash
# View component logs
sudo tail -f /greengrass/v2/logs/com.example.EchoComponent.log

# View Greengrass system logs
sudo tail -f /greengrass/v2/logs/greengrass.log
```

## Monitoring Metrics

The component publishes custom metrics to CloudWatch:

- **MessagesReceived**: Total messages received
- **MessagesSent**: Total messages sent
- **Errors**: Total errors encountered
- **ReceiveRate**: Messages received per second

View metrics in CloudWatch Console:
1. Go to CloudWatch → Metrics → Greengrass
2. Look for metrics from `com.example.EchoComponent`

## Updating Configuration

Update the component configuration via deployment:

```bash
aws greengrassv2 create-deployment \
    --target-arn "arn:aws:iot:region:account:thing/MyGreengrassCoreDevice" \
    --components '{
        "com.example.EchoComponent": {
            "componentVersion": "1.0.0",
            "configurationUpdate": {
                "merge": "{\"prefix\":\"[UPDATED]\",\"logIntervalSeconds\":30}"
            }
        }
    }'
```

The component will automatically reload its configuration without restarting!

## Troubleshooting

### Component Not Starting

1. Check Greengrass logs: `/greengrass/v2/logs/greengrass.log`
2. Verify the binary has execute permissions
3. Check that IPC environment variables are set:
   ```bash
   echo $AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT
   echo $SVCUID
   ```

### Not Receiving Messages

1. Verify IoT Core permissions in the recipe's `accessControl` section
2. Check that topics match what you're publishing to
3. Verify the device has IoT Core connectivity

### Metrics Not Appearing

1. Ensure the component has CloudWatch permissions
2. Check that the Greengrass service role has `cloudwatch:PutMetricData` permission
3. Wait a few minutes for metrics to propagate

## Code Structure

```
main.go
├── Component struct       - Main component state
├── NewComponent()        - Creates and initializes component
├── LoadConfiguration()   - Loads config from Greengrass
├── ReportRunning()       - Reports lifecycle state
├── Run()                 - Main event loop
├── subscribeToInput()    - Subscribes to and processes messages
├── handleMessage()       - Processes individual messages
├── reportMetrics()       - Publishes metrics periodically
├── listenForConfigUpdates() - Handles config changes
└── Shutdown()            - Graceful shutdown
```

## Extending the Example

Ideas for extending this component:

1. **Message Transformation**: Parse JSON and transform data
2. **Local Pub/Sub**: Communicate with other components locally
3. **Shadow Integration**: Update device shadow with component state
4. **Secrets**: Retrieve API keys from AWS Secrets Manager
5. **Component Updates**: Handle pre/post update events
6. **Validation**: Subscribe to configuration validation requests

## Related Documentation

- [AWS IoT Greengrass Documentation](https://docs.aws.amazon.com/greengrass/)
- [Greengrass Component Recipe Reference](https://docs.aws.amazon.com/greengrass/v2/developerguide/component-recipe-reference.html)
- [IPC Service Reference](https://docs.aws.amazon.com/greengrass/v2/developerguide/interprocess-communication.html)
