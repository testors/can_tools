/**
 * @file main.m
 * @brief CAN Reader - BLE/Classic BT OBD-II tool
 *
 * Connects to ELM327-compatible OBD adapters (OBDLink MX+, vLinker MC+)
 * via CoreBluetooth/IOBluetooth and provides interactive PID querying,
 * CAN monitoring, bus detection, and signal discovery.
 *
 * Usage: can_reader <command> [args...]
 */

#import <Foundation/Foundation.h>
#import <CoreBluetooth/CoreBluetooth.h>
#import <IOBluetooth/IOBluetooth.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <signal.h>
#include <sys/time.h>

#include "elm327.h"
#include "stn2255.h"
#include "can_discover.h"

/*******************************************************************************
 * Constants
 ******************************************************************************/

/** SPP-over-BLE Service UUID (OBDLink, vLinker) */
static NSString * const kServiceUUID = @"FFF0";

/** BLE device name prefixes for auto-detection */
static NSArray<NSString *> *kDeviceNamePrefixes;

/** Response timeout in seconds */
static const NSTimeInterval kResponseTimeout = 3.0;

/** Response buffer size */
#define RESPONSE_BUF_SIZE 4096

/** Max discovered devices */
#define MAX_DEVICES 16

/** Transport type for discovered devices */
typedef enum { OBD_TRANSPORT_BLE, OBD_TRANSPORT_CLASSIC } OBDTransportType;

/*******************************************************************************
 * CLI Mode State (declared early for use in BLE delegate methods)
 ******************************************************************************/

static bool g_cli_mode = false;

#define CLI_MAX_CAN_FRAMES 4096
static can_frame_t g_cli_can_frames[CLI_MAX_CAN_FRAMES];
static int g_cli_can_frame_count = 0;

/** Local counters (replaces obd_provider) */
static uint32_t g_sample_count = 0;
static uint32_t g_can_frame_count = 0;

/*******************************************************************************
 * BLE Manager (Objective-C)
 ******************************************************************************/

@interface BLEManager : NSObject <CBCentralManagerDelegate, CBPeripheralDelegate,
                                   IOBluetoothDeviceInquiryDelegate,
                                   IOBluetoothRFCOMMChannelDelegate> {
@public
    char _responseBuf[RESPONSE_BUF_SIZE];
    int _responseLen;
    BOOL _responseReady;
    BOOL _bleReady;
    char _canLineBuf[256];
    int _canLineLen;
}

@property (nonatomic, strong) CBCentralManager *central;
@property (nonatomic, strong) NSMutableArray<CBPeripheral *> *discoveredDevices;
@property (nonatomic, strong) NSMutableArray<NSString *> *discoveredNames;
@property (nonatomic, strong) NSMutableArray<NSNumber *> *discoveredRSSIs;
@property (nonatomic, strong) CBPeripheral *connectedPeripheral;
@property (nonatomic, strong) CBCharacteristic *rxCharacteristic;
@property (nonatomic, strong) CBCharacteristic *txCharacteristic;

/* Classic Bluetooth */
@property (nonatomic, strong) IOBluetoothDeviceInquiry *classicInquiry;
@property (nonatomic, strong) NSMutableArray<IOBluetoothDevice *> *classicDevices;
@property (nonatomic, strong) IOBluetoothDevice *connectedClassicDevice;
@property (nonatomic, strong) IOBluetoothRFCOMMChannel *rfcommChannel;
@property (nonatomic, assign) BOOL classicInquiryComplete;

/* Unified device list tracking */
@property (nonatomic, strong) NSMutableArray<NSNumber *> *discoveredTransports;
@property (nonatomic, strong) NSMutableArray<NSNumber *> *discoveredSourceIndices;
@property (nonatomic, assign) OBDTransportType activeTransport;

@property (nonatomic, assign) BOOL isScanning;
@property (nonatomic, assign) BOOL isConnected;
@property (nonatomic, assign) BOOL isConnecting;
@property (nonatomic, assign) BOOL serviceDiscoveryDone;

/* CAN monitor mode */
@property (nonatomic, assign) BOOL canMonitorActive;

@property (nonatomic, readonly) NSString *connectedDeviceName;

- (BOOL)isBleReady;
- (BOOL)isResponseReady;
- (void)startScan;
- (void)stopScan;
- (BOOL)connectToDevice:(int)index;
- (void)disconnect;
- (BOOL)sendCommand:(const char *)cmd response:(char *)resp maxLen:(int)maxLen;
- (void)sendRawString:(const char *)str;

@end

@implementation BLEManager

- (instancetype)init {
    self = [super init];
    if (self) {
        _discoveredDevices = [NSMutableArray array];
        _discoveredNames = [NSMutableArray array];
        _discoveredRSSIs = [NSMutableArray array];
        _classicDevices = [NSMutableArray array];
        _discoveredTransports = [NSMutableArray array];
        _discoveredSourceIndices = [NSMutableArray array];
        _central = [[CBCentralManager alloc] initWithDelegate:self
                                                        queue:nil
                                                      options:@{CBCentralManagerOptionShowPowerAlertKey: @YES}];
        _classicInquiry = [IOBluetoothDeviceInquiry inquiryWithDelegate:self];
        _classicInquiry.inquiryLength = 4;
        _classicInquiry.updateNewDeviceNames = YES;
    }
    return self;
}

- (NSString *)connectedDeviceName {
    if (_activeTransport == OBD_TRANSPORT_CLASSIC) {
        return _connectedClassicDevice.name;
    }
    return _connectedPeripheral.name;
}

- (BOOL)isBleReady {
    return _bleReady;
}

- (BOOL)isResponseReady {
    return _responseReady;
}

/*******************************************************************************
 * CBCentralManagerDelegate
 ******************************************************************************/

- (void)centralManagerDidUpdateState:(CBCentralManager *)central {
    if (central.state == CBManagerStatePoweredOn) {
        _bleReady = YES;
        if (!g_cli_mode) printf("  Bluetooth is ready.\n");
    } else {
        _bleReady = NO;
        if (g_cli_mode) {
            fprintf(stderr, "[WARN] Bluetooth not available (state: %ld)\n", (long)central.state);
        } else {
            printf("  Bluetooth not available (state: %ld)\n", (long)central.state);
        }
    }
}

- (void)centralManager:(CBCentralManager *)central
 didDiscoverPeripheral:(CBPeripheral *)peripheral
     advertisementData:(NSDictionary<NSString *,id> *)advertisementData
                  RSSI:(NSNumber *)RSSI {
    NSString *name = peripheral.name;
    if (!name || name.length == 0) return;

    /* Check for duplicate */
    for (CBPeripheral *existing in _discoveredDevices) {
        if ([existing.identifier isEqual:peripheral.identifier]) return;
    }

    /* Filter by known device name prefixes */
    BOOL matched = NO;
    for (NSString *prefix in kDeviceNamePrefixes) {
        if ([[name uppercaseString] containsString:[prefix uppercaseString]]) {
            matched = YES;
            break;
        }
    }

    if (matched) {
        NSUInteger bleIdx = _discoveredDevices.count;
        [_discoveredDevices addObject:peripheral];
        [_discoveredNames addObject:name];
        [_discoveredRSSIs addObject:RSSI];
        [_discoveredTransports addObject:@(OBD_TRANSPORT_BLE)];
        [_discoveredSourceIndices addObject:@(bleIdx)];
        if (!g_cli_mode) {
            printf("  [%lu] %s (BLE, RSSI: %d dBm)\n",
                   (unsigned long)(_discoveredNames.count - 1),
                   name.UTF8String,
                   RSSI.intValue);
        }
    }
}

- (void)centralManager:(CBCentralManager *)central
  didConnectPeripheral:(CBPeripheral *)peripheral {
    if (!g_cli_mode) printf("  Connected to %s\n", peripheral.name.UTF8String);
    _isConnecting = NO;
    _isConnected = YES;
    _connectedPeripheral = peripheral;
    peripheral.delegate = self;

    /* Discover services */
    if (!g_cli_mode) printf("  Discovering services...\n");
    [peripheral discoverServices:@[[CBUUID UUIDWithString:kServiceUUID]]];
}

- (void)centralManager:(CBCentralManager *)central
didFailToConnectPeripheral:(CBPeripheral *)peripheral
                 error:(NSError *)error {
    if (g_cli_mode) {
        fprintf(stderr, "[ERROR] Connection failed: %s\n", error.localizedDescription.UTF8String);
    } else {
        printf("  Connection failed: %s\n", error.localizedDescription.UTF8String);
    }
    _isConnecting = NO;
}

- (void)centralManager:(CBCentralManager *)central
didDisconnectPeripheral:(CBPeripheral *)peripheral
                 error:(NSError *)error {
    if (!g_cli_mode) {
        printf("  Disconnected from %s\n", peripheral.name.UTF8String ?: "device");
    }
    _isConnected = NO;
    _connectedPeripheral = nil;
    _rxCharacteristic = nil;
    _txCharacteristic = nil;
    _serviceDiscoveryDone = NO;
}

/*******************************************************************************
 * CBPeripheralDelegate
 ******************************************************************************/

- (void)peripheral:(CBPeripheral *)peripheral
didDiscoverServices:(NSError *)error {
    if (error) {
        if (g_cli_mode) {
            fprintf(stderr, "[ERROR] Service discovery error: %s\n", error.localizedDescription.UTF8String);
        } else {
            printf("  Service discovery error: %s\n", error.localizedDescription.UTF8String);
        }
        return;
    }

    for (CBService *service in peripheral.services) {
        if (!g_cli_mode) printf("  Found service: %s\n", service.UUID.UUIDString.UTF8String);
        [peripheral discoverCharacteristics:nil forService:service];
    }
}

- (void)peripheral:(CBPeripheral *)peripheral
didDiscoverCharacteristicsForService:(CBService *)service
             error:(NSError *)error {
    if (error) {
        if (g_cli_mode) {
            fprintf(stderr, "[ERROR] Characteristic discovery error: %s\n", error.localizedDescription.UTF8String);
        } else {
            printf("  Characteristic discovery error: %s\n", error.localizedDescription.UTF8String);
        }
        return;
    }

    for (CBCharacteristic *characteristic in service.characteristics) {
        NSString *uuid = characteristic.UUID.UUIDString;

        /* Identify by UUID */
        if ([[uuid uppercaseString] containsString:@"FFF1"]) {
            _rxCharacteristic = characteristic;
            if (!g_cli_mode) printf("  Found characteristic: %s (RX - write)\n", uuid.UTF8String);
        } else if ([[uuid uppercaseString] containsString:@"FFF2"]) {
            _txCharacteristic = characteristic;
            if (!g_cli_mode) printf("  Found characteristic: %s (TX - notify)\n", uuid.UTF8String);
            /* Subscribe to notifications */
            [peripheral setNotifyValue:YES forCharacteristic:characteristic];
        } else {
            if (!g_cli_mode) printf("  Found characteristic: %s\n", uuid.UTF8String);
        }
    }

    if (_rxCharacteristic && _txCharacteristic) {
        if (!g_cli_mode) printf("  Service discovery complete. Ready for commands.\n");
        _serviceDiscoveryDone = YES;
    } else {
        if (g_cli_mode) {
            fprintf(stderr, "[WARN] Missing required characteristics (RX=%s, TX=%s)\n",
                    _rxCharacteristic ? "OK" : "missing",
                    _txCharacteristic ? "OK" : "missing");
        } else {
            printf("  Warning: Missing required characteristics (RX=%s, TX=%s)\n",
                   _rxCharacteristic ? "OK" : "missing",
                   _txCharacteristic ? "OK" : "missing");
        }
    }
}

/** Shared data handler for both BLE and Classic transports */
- (void)_handleReceivedBytes:(const char *)bytes length:(int)len {
    if (_canMonitorActive) {
        /* In CAN monitor mode: parse lines across callbacks */
        for (int i = 0; i < len; i++) {
            if (bytes[i] == '\r' || bytes[i] == '\n') {
                if (_canLineLen > 0) {
                    _canLineBuf[_canLineLen] = '\0';

                    /* Parse CAN frame */
                    can_frame_t frame;
                    if (elm327_parse_can_frame(_canLineBuf, &frame)) {
                        g_can_frame_count++;
                        if (g_cli_mode) {
                            /* CLI mode: store frames for JSON output */
                            if (g_cli_can_frame_count < CLI_MAX_CAN_FRAMES) {
                                g_cli_can_frames[g_cli_can_frame_count++] = frame;
                            }
                        } else {
                            printf("  %03X [%d] ", frame.can_id, frame.dlc);
                            for (int j = 0; j < frame.dlc; j++) {
                                printf("%02X ", frame.data[j]);
                            }
                            printf("\n");
                        }
                    }
                    _canLineLen = 0;
                }
            } else if (bytes[i] != '>' && _canLineLen < 255) {
                _canLineBuf[_canLineLen++] = bytes[i];
            }
        }
        return;
    }

    /* Normal mode: accumulate response until '>' prompt */
    for (int i = 0; i < len && _responseLen < RESPONSE_BUF_SIZE - 1; i++) {
        _responseBuf[_responseLen++] = bytes[i];

        if (bytes[i] == '>') {
            _responseBuf[_responseLen] = '\0';
            _responseReady = YES;
        }
    }
}

- (void)peripheral:(CBPeripheral *)peripheral
didUpdateValueForCharacteristic:(CBCharacteristic *)characteristic
             error:(NSError *)error {
    if (error) return;

    NSData *data = characteristic.value;
    if (!data || data.length == 0) return;

    [self _handleReceivedBytes:(const char *)data.bytes length:(int)data.length];
}

- (void)peripheral:(CBPeripheral *)peripheral
didUpdateNotificationStateForCharacteristic:(CBCharacteristic *)characteristic
             error:(NSError *)error {
    if (g_cli_mode) {
        if (error) {
            fprintf(stderr, "[ERROR] Notification error: %s\n", error.localizedDescription.UTF8String);
        }
    } else {
        if (error) {
            printf("  Notification error: %s\n", error.localizedDescription.UTF8String);
        } else {
            printf("  Notifications %s for %s\n",
                   characteristic.isNotifying ? "enabled" : "disabled",
                   characteristic.UUID.UUIDString.UTF8String);
        }
    }
}

/*******************************************************************************
 * IOBluetoothDeviceInquiryDelegate (Classic BT discovery)
 ******************************************************************************/

- (void)_addClassicDeviceIfMatched:(IOBluetoothDevice *)device {
    NSString *name = device.name;
    if (!name || name.length == 0) return;

    /* Check for duplicate */
    for (IOBluetoothDevice *existing in _classicDevices) {
        if ([existing.addressString isEqualToString:device.addressString]) return;
    }

    /* Filter by known device name prefixes */
    BOOL matched = NO;
    for (NSString *prefix in kDeviceNamePrefixes) {
        if ([[name uppercaseString] containsString:[prefix uppercaseString]]) {
            matched = YES;
            break;
        }
    }

    if (matched) {
        NSUInteger classicIdx = _classicDevices.count;
        [_classicDevices addObject:device];
        [_discoveredNames addObject:name];
        [_discoveredRSSIs addObject:@(device.rawRSSI)];
        [_discoveredTransports addObject:@(OBD_TRANSPORT_CLASSIC)];
        [_discoveredSourceIndices addObject:@(classicIdx)];
        if (!g_cli_mode) {
            printf("  [%lu] %s (Classic BT)\n",
                   (unsigned long)(_discoveredNames.count - 1),
                   name.UTF8String);
        }
    }
}

- (void)deviceInquiryDeviceFound:(IOBluetoothDeviceInquiry *)sender
                          device:(IOBluetoothDevice *)device {
    [self _addClassicDeviceIfMatched:device];
}

- (void)deviceInquiryDeviceNameUpdated:(IOBluetoothDeviceInquiry *)sender
                                device:(IOBluetoothDevice *)device
                      devicesRemaining:(uint32_t)devicesRemaining {
    [self _addClassicDeviceIfMatched:device];
}

- (void)deviceInquiryComplete:(IOBluetoothDeviceInquiry *)sender
                        error:(IOReturn)error
                      aborted:(BOOL)aborted {
    _classicInquiryComplete = YES;
}

/*******************************************************************************
 * IOBluetoothRFCOMMChannelDelegate (Classic BT data)
 ******************************************************************************/

- (void)rfcommChannelOpenComplete:(IOBluetoothRFCOMMChannel *)rfcommChannel
                           status:(IOReturn)error {
    _isConnecting = NO;

    if (error != kIOReturnSuccess) {
        if (g_cli_mode) {
            fprintf(stderr, "[ERROR] RFCOMM open failed (0x%08x)\n", error);
        } else {
            printf("  RFCOMM channel open failed.\n");
        }
        return;
    }

    if (!g_cli_mode) printf("  Connected via Classic BT (RFCOMM)\n");
    _isConnected = YES;
    _serviceDiscoveryDone = YES;
}

- (void)rfcommChannelData:(IOBluetoothRFCOMMChannel *)rfcommChannel
                     data:(void *)dataPointer
                   length:(size_t)dataLength {
    [self _handleReceivedBytes:(const char *)dataPointer length:(int)dataLength];
}

- (void)rfcommChannelClosed:(IOBluetoothRFCOMMChannel *)rfcommChannel {
    if (!g_cli_mode) {
        printf("  Classic BT disconnected\n");
    }
    _isConnected = NO;
    _connectedClassicDevice = nil;
    _rfcommChannel = nil;
    _serviceDiscoveryDone = NO;
}

/*******************************************************************************
 * Public Methods
 ******************************************************************************/

- (void)startScan {
    [_discoveredDevices removeAllObjects];
    [_discoveredNames removeAllObjects];
    [_discoveredRSSIs removeAllObjects];
    [_classicDevices removeAllObjects];
    [_discoveredTransports removeAllObjects];
    [_discoveredSourceIndices removeAllObjects];
    _classicInquiryComplete = NO;
    _isScanning = YES;

    if (!g_cli_mode) printf("  Scanning for OBD devices (BLE + Classic)...\n");

    if (_bleReady) {
        [_central scanForPeripheralsWithServices:nil options:@{
            CBCentralManagerScanOptionAllowDuplicatesKey: @NO
        }];
    } else {
        if (!g_cli_mode) printf("  (BLE not available, scanning Classic only)\n");
    }

    [_classicInquiry start];
}

- (void)stopScan {
    [_central stopScan];
    [_classicInquiry stop];
    _isScanning = NO;
}

- (BOOL)connectToDevice:(int)index {
    if (index < 0 || index >= (int)_discoveredNames.count) {
        if (g_cli_mode) {
            fprintf(stderr, "[ERROR] Invalid device index\n");
        } else {
            printf("  Invalid device index.\n");
        }
        return NO;
    }

    if (_isConnected) {
        if (g_cli_mode) {
            fprintf(stderr, "[ERROR] Already connected. Disconnect first\n");
        } else {
            printf("  Already connected. Disconnect first.\n");
        }
        return NO;
    }

    OBDTransportType transport = (OBDTransportType)[_discoveredTransports[index] intValue];
    int sourceIdx = [_discoveredSourceIndices[index] intValue];

    if (transport == OBD_TRANSPORT_BLE) {
        CBPeripheral *peripheral = _discoveredDevices[sourceIdx];
        if (!g_cli_mode) printf("  Connecting to %s (BLE)...\n", peripheral.name.UTF8String);
        _activeTransport = OBD_TRANSPORT_BLE;
        _isConnecting = YES;
        [_central connectPeripheral:peripheral options:nil];
    } else {
        IOBluetoothDevice *device = _classicDevices[sourceIdx];
        if (!g_cli_mode) printf("  Connecting to %s (Classic BT)...\n", device.name.UTF8String);
        _activeTransport = OBD_TRANSPORT_CLASSIC;
        _isConnecting = YES;

        /* Open RFCOMM channel synchronously.
           Handles baseband connection internally.
           SPP channel is typically 1 for OBD adapters. */
        IOBluetoothRFCOMMChannel *channel = nil;
        IOReturn result = [device openRFCOMMChannelSync:&channel
                                          withChannelID:1
                                               delegate:self];
        _isConnecting = NO;

        if (result != kIOReturnSuccess || channel == nil) {
            if (g_cli_mode) {
                fprintf(stderr, "[ERROR] RFCOMM connection failed (0x%08x)\n", result);
            } else {
                printf("  RFCOMM connection failed (0x%08x).\n", result);
            }
            return NO;
        }

        _rfcommChannel = channel;
        _connectedClassicDevice = device;
        _isConnected = YES;
        _serviceDiscoveryDone = YES;
        if (!g_cli_mode) printf("  Connected via Classic BT (RFCOMM)\n");
    }

    return YES;
}

- (void)disconnect {
    if (_activeTransport == OBD_TRANSPORT_CLASSIC) {
        if (_rfcommChannel) {
            [_rfcommChannel closeChannel];
            _rfcommChannel = nil;
        }
        if (_connectedClassicDevice) {
            [_connectedClassicDevice closeConnection];
            _connectedClassicDevice = nil;
        }
    } else {
        if (_connectedPeripheral) {
            [_central cancelPeripheralConnection:_connectedPeripheral];
        }
        _connectedPeripheral = nil;
        _rxCharacteristic = nil;
        _txCharacteristic = nil;
    }
    _isConnected = NO;
    _serviceDiscoveryDone = NO;
    _canMonitorActive = NO;
}

- (BOOL)sendCommand:(const char *)cmd response:(char *)resp maxLen:(int)maxLen {
    if (!_isConnected || !_serviceDiscoveryDone) {
        if (g_cli_mode) {
            fprintf(stderr, "[ERROR] Not connected or service not ready\n");
        } else {
            printf("  Not connected or service not ready.\n");
        }
        return NO;
    }

    if (_activeTransport == OBD_TRANSPORT_BLE && !_rxCharacteristic) {
        if (g_cli_mode) {
            fprintf(stderr, "[ERROR] BLE characteristic not ready\n");
        } else {
            printf("  BLE characteristic not ready.\n");
        }
        return NO;
    }

    /* Clear response buffer */
    memset(_responseBuf, 0, sizeof(_responseBuf));
    _responseLen = 0;
    _responseReady = NO;

    /* Send command with CR */
    char cmdWithCr[128];
    snprintf(cmdWithCr, sizeof(cmdWithCr), "%s\r", cmd);

    if (_activeTransport == OBD_TRANSPORT_CLASSIC) {
        [_rfcommChannel writeSync:(void *)cmdWithCr length:strlen(cmdWithCr)];
    } else {
        NSData *data = [NSData dataWithBytes:cmdWithCr length:strlen(cmdWithCr)];

        CBCharacteristicWriteType writeType =
            (_rxCharacteristic.properties & CBCharacteristicPropertyWriteWithoutResponse)
                ? CBCharacteristicWriteWithoutResponse
                : CBCharacteristicWriteWithResponse;

        [_connectedPeripheral writeValue:data
                       forCharacteristic:_rxCharacteristic
                                    type:writeType];
    }

    /* Wait for response with timeout */
    NSDate *timeout = [NSDate dateWithTimeIntervalSinceNow:kResponseTimeout];
    while (!_responseReady && [[NSDate date] compare:timeout] == NSOrderedAscending) {
        [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                 beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.01]];
    }

    if (!_responseReady) {
        if (g_cli_mode) {
            fprintf(stderr, "[WARN] Command timeout: %s\n", cmd);
        } else {
            printf("  Command timeout: %s\n", cmd);
        }
        return NO;
    }

    /* Clean response: remove echo, prompt, whitespace */
    char *src = _responseBuf;
    int dstLen = 0;

    /* Skip echo of command */
    char *echoEnd = strstr(src, cmd);
    if (echoEnd) {
        src = echoEnd + strlen(cmd);
    }

    /* Copy cleaned response */
    for (int i = 0; src[i] && dstLen < maxLen - 1; i++) {
        if (src[i] == '>') continue;
        if (src[i] == '\r') {
            if (dstLen > 0 && resp[dstLen - 1] != '\n') {
                resp[dstLen++] = '\n';
            }
            continue;
        }
        resp[dstLen++] = src[i];
    }
    resp[dstLen] = '\0';

    /* Trim trailing whitespace */
    while (dstLen > 0 && (resp[dstLen - 1] == '\n' || resp[dstLen - 1] == ' ')) {
        resp[--dstLen] = '\0';
    }

    return YES;
}

- (void)sendRawString:(const char *)str {
    if (!_isConnected) return;

    if (_activeTransport == OBD_TRANSPORT_CLASSIC) {
        if (_rfcommChannel) {
            [_rfcommChannel writeSync:(void *)str length:strlen(str)];
        }
    } else {
        if (!_rxCharacteristic) return;
        NSData *data = [NSData dataWithBytes:str length:strlen(str)];
        CBCharacteristicWriteType writeType =
            (_rxCharacteristic.properties & CBCharacteristicPropertyWriteWithoutResponse)
                ? CBCharacteristicWriteWithoutResponse
                : CBCharacteristicWriteWithResponse;

        [_connectedPeripheral writeValue:data
                       forCharacteristic:_rxCharacteristic
                                    type:writeType];
    }
}

@end

/*******************************************************************************
 * Global State
 ******************************************************************************/

static BLEManager *g_ble = nil;
static volatile BOOL g_running = YES;
static volatile BOOL g_interrupt = NO;

/* STN2255 state */
static stn2255_info_t g_stn_info = {0};
static bool g_stn_detected = false;

static void signal_handler(int sig) {
    (void)sig;
    if (g_ble.canMonitorActive) {
        g_interrupt = YES;
    } else {
        g_running = NO;
    }
}

/*******************************************************************************
 * Helper Functions
 ******************************************************************************/

static int64_t get_timestamp_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

/** Run the event loop for a given duration */
static void run_loop_for(double seconds) {
    NSDate *until = [NSDate dateWithTimeIntervalSinceNow:seconds];
    while ([[NSDate date] compare:until] == NSOrderedAscending) {
        @autoreleasepool {
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                     beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.01]];
        }
    }
}

/** Wait for a condition with timeout, pumping run loop */
static BOOL wait_for(BOOL *condition, double timeoutSeconds) {
    NSDate *timeout = [NSDate dateWithTimeIntervalSinceNow:timeoutSeconds];
    while (!(*condition) && [[NSDate date] compare:timeout] == NSOrderedAscending) {
        @autoreleasepool {
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                     beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.01]];
        }
    }
    return *condition;
}

/*******************************************************************************
 * Menu Actions
 ******************************************************************************/

static void action_scan(void) {
    [g_ble startScan];
    printf("  Scanning for 5 seconds...\n\n");
    run_loop_for(5.0);
    [g_ble stopScan];
    printf("\n  Found %lu device(s).\n", (unsigned long)g_ble.discoveredNames.count);
}

static void action_connect(void) {
    if (g_ble.discoveredNames.count == 0) {
        printf("  No devices found. Scan first.\n");
        return;
    }

    printf("  Enter device number [0-%lu]: ",
           (unsigned long)(g_ble.discoveredNames.count - 1));
    fflush(stdout);

    char input[16];
    if (!fgets(input, sizeof(input), stdin)) return;
    int idx = atoi(input);

    if ([g_ble connectToDevice:idx]) {
        /* Wait for connection + service discovery */
        printf("  Waiting for connection...\n");
        BOOL connected = NO;

        NSDate *timeout = [NSDate dateWithTimeIntervalSinceNow:10.0];
        while (!g_ble.serviceDiscoveryDone &&
               [[NSDate date] compare:timeout] == NSOrderedAscending) {
            @autoreleasepool {
                [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                         beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.05]];
            }
        }

        connected = g_ble.serviceDiscoveryDone;
        if (connected) {
            printf("  Connection successful.\n");
        } else {
            printf("  Connection/service discovery timeout.\n");
        }
    }
}

static void action_init_elm327(void) {
    if (!g_ble.isConnected || !g_ble.serviceDiscoveryDone) {
        printf("  Not connected.\n");
        return;
    }

    printf("  Initializing ELM327...\n\n");

    char cmds[ELM327_MAX_INIT_CMDS][ELM327_CMD_MAX_LEN];
    int cmd_count;
    elm327_build_init_cmds(cmds, &cmd_count);

    char resp[512];

    for (int i = 0; i < cmd_count; i++) {
        printf("  >> %s\n", cmds[i]);

        if ([g_ble sendCommand:cmds[i] response:resp maxLen:sizeof(resp)]) {
            printf("  << %s\n\n", resp);

            /* Parse specific responses */
            if (strcmp(cmds[i], "ATZ") == 0) {
                /* After reset, add extra delay */
                run_loop_for(0.5);
            } else if (strcmp(cmds[i], "ATRV") == 0) {
                double voltage;
                if (elm327_parse_voltage(resp, &voltage)) {
                    printf("  Battery Voltage: %.1f V\n\n", voltage);
                }
            } else if (strcmp(cmds[i], "ATDPN") == 0) {
                const char *proto = elm327_parse_protocol(resp);
                if (proto) {
                    printf("  Protocol: %s\n\n", proto);
                }
            }
        } else {
            printf("  << (timeout)\n\n");
        }
    }

    printf("  ELM327 initialization complete.\n");

    /* Try STN detection */
    printf("\n  Detecting STN chip...\n");
    char sti_resp[256];
    printf("  >> STI\n");
    if ([g_ble sendCommand:"STI" response:sti_resp maxLen:sizeof(sti_resp)]) {
        printf("  << %s\n", sti_resp);

        if (stn2255_detect(resp, sti_resp, &g_stn_info)) {
            g_stn_detected = true;
            printf("\n  STN detected: %s\n", g_stn_info.device_id);

            /* Send STDI for hardware ID */
            char stdi_resp[256];
            printf("  >> STDI\n");
            if ([g_ble sendCommand:"STDI" response:stdi_resp maxLen:sizeof(stdi_resp)]) {
                printf("  << %s\n", stdi_resp);
                /* Copy hardware ID */
                const char *s = stdi_resp;
                while (*s == ' ' || *s == '\r' || *s == '\n') s++;
                strncpy(g_stn_info.hardware_id, s, sizeof(g_stn_info.hardware_id) - 1);
            }

            /* Send STN optimization commands */
            char stn_cmds[STN2255_MAX_INIT_CMDS][STN2255_CMD_MAX_LEN];
            int stn_cmd_count;
            stn2255_build_init_cmds(stn_cmds, &stn_cmd_count);

            printf("\n  Applying STN optimizations...\n\n");
            for (int i = 0; i < stn_cmd_count; i++) {
                printf("  >> %s\n", stn_cmds[i]);
                char stn_resp[256];
                if ([g_ble sendCommand:stn_cmds[i] response:stn_resp maxLen:sizeof(stn_resp)]) {
                    printf("  << %s\n\n", stn_resp);
                } else {
                    printf("  << (timeout)\n\n");
                }
            }

            printf("  STN initialization complete.\n");
        } else {
            printf("  Not an STN chip (standard ELM327).\n");
        }
    } else {
        printf("  << (timeout) - STI not supported, standard ELM327.\n");
    }
}

static void action_query_supported_pids(void) {
    if (!g_ble.isConnected || !g_ble.serviceDiscoveryDone) {
        printf("  Not connected.\n");
        return;
    }

    bool supported[256];
    memset(supported, 0, sizeof(supported));

    printf("  Querying supported PIDs...\n\n");

    /* Query PID 0x00 (supported PIDs 01-20) */
    static const uint8_t query_pids[] = { 0x00, 0x20, 0x40, 0x60 };
    static const char *query_labels[] = {
        "PIDs 01-20", "PIDs 21-40", "PIDs 41-60", "PIDs 61-80"
    };

    for (int q = 0; q < 4; q++) {
        char cmd[16];
        elm327_build_pid_query(query_pids[q], cmd, sizeof(cmd));

        char resp[256];
        printf("  >> %s (%s)\n", cmd, query_labels[q]);

        if ([g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)]) {
            if (!elm327_is_error_response(resp)) {
                printf("  << %s\n", resp);
                elm327_parse_supported_pids(resp, query_pids[q], supported);
            } else {
                printf("  << %s (not supported)\n", resp);
                break; /* Stop if range not supported */
            }
        } else {
            printf("  << (timeout)\n");
            break;
        }
    }

    /* Print supported PIDs */
    printf("\n  Supported PIDs:\n");
    int count = 0;
    for (int i = 1; i < 256; i++) {
        if (supported[i]) {
            const elm327_pid_info_t *info = elm327_get_pid_info(i);
            if (info) {
                printf("    0x%02X  %-25s [%s]\n", i, info->name, info->unit);
            } else {
                printf("    0x%02X  (unknown)\n", i);
            }
            count++;
        }
    }
    printf("\n  Total: %d supported PIDs.\n", count);
}

static void action_poll_single_pid(void) {
    if (!g_ble.isConnected || !g_ble.serviceDiscoveryDone) {
        printf("  Not connected.\n");
        return;
    }

    printf("  Enter PID hex (e.g., 0C for RPM): ");
    fflush(stdout);

    char input[16];
    if (!fgets(input, sizeof(input), stdin)) return;

    /* Parse hex PID */
    unsigned int pid_val;
    if (sscanf(input, "%x", &pid_val) != 1 || pid_val > 0xFF) {
        printf("  Invalid PID.\n");
        return;
    }
    uint8_t pid = (uint8_t)pid_val;

    const elm327_pid_info_t *info = elm327_get_pid_info(pid);
    printf("  Querying PID 0x%02X (%s)...\n",
           pid, info ? info->name : "unknown");

    char cmd[16];
    elm327_build_pid_query(pid, cmd, sizeof(cmd));

    char resp[256];
    printf("  >> %s\n", cmd);

    if ([g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)]) {
        printf("  << %s\n", resp);

        if (!elm327_is_error_response(resp)) {
            uint8_t parsed_pid;
            uint8_t data[ELM327_MAX_DATA_BYTES];
            int data_len;

            if (elm327_parse_pid_response(resp, &parsed_pid, data, &data_len)) {
                double value = elm327_convert_pid_value(parsed_pid, data, data_len);
                printf("\n  Result: %.2f %s\n",
                       value, info ? info->unit : "");
            } else {
                printf("  Failed to parse response.\n");
            }
        } else {
            printf("  Error response from adapter.\n");
        }
    } else {
        printf("  Command timeout.\n");
    }
}

static void action_poll_continuous(void) {
    if (!g_ble.isConnected || !g_ble.serviceDiscoveryDone) {
        printf("  Not connected.\n");
        return;
    }

    /* Motorsports basic PIDs: RPM, Throttle, Speed */
    static const uint8_t poll_pids[] = { 0x0C, 0x11, 0x0D };
    static const int poll_count = 3;

    printf("  Continuous polling: RPM, Throttle, Speed\n");
    if (g_stn_detected) {
        printf("  Mode: batch query (1 round-trip)\n");
    } else {
        printf("  Mode: sequential query (%d round-trips)\n", poll_count);
    }
    printf("  Press Ctrl+C to stop.\n\n");

    g_interrupt = NO;

    while (!g_interrupt && g_ble.isConnected) {
        @autoreleasepool {
            printf("\r  ");

            if (g_stn_detected) {
                /* Batch query: single round-trip */
                char cmd[64];
                stn2255_build_batch_query(poll_pids, poll_count, cmd, sizeof(cmd));

                char resp[512];
                if ([g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)] &&
                    !elm327_is_error_response(resp)) {

                    stn2255_batch_result_t batch;
                    int parsed = stn2255_parse_batch_response(resp, &batch);

                    if (parsed > 0) {
                        for (int i = 0; i < batch.count; i++) {
                            if (!batch.results[i].valid) continue;
                            const elm327_pid_info_t *info = elm327_get_pid_info(batch.results[i].pid);
                            if (info) {
                                printf("%-15s: %7.1f %-5s  ",
                                       info->name, batch.results[i].value, info->unit);
                            }
                            g_sample_count++;
                        }

                        /* Fallback: if fewer PIDs returned than requested, poll missing ones */
                        if (batch.count < poll_count) {
                            for (int i = 0; i < poll_count && !g_interrupt; i++) {
                                bool found = false;
                                for (int j = 0; j < batch.count; j++) {
                                    if (batch.results[j].pid == poll_pids[i] && batch.results[j].valid) {
                                        found = true;
                                        break;
                                    }
                                }
                                if (!found) {
                                    /* Sequential fallback for missing PID */
                                    char single_cmd[16];
                                    elm327_build_pid_query(poll_pids[i], single_cmd, sizeof(single_cmd));
                                    char single_resp[256];
                                    if ([g_ble sendCommand:single_cmd response:single_resp maxLen:sizeof(single_resp)] &&
                                        !elm327_is_error_response(single_resp)) {
                                        uint8_t pid;
                                        uint8_t data[ELM327_MAX_DATA_BYTES];
                                        int data_len;
                                        if (elm327_parse_pid_response(single_resp, &pid, data, &data_len)) {
                                            double value = elm327_convert_pid_value(pid, data, data_len);
                                            const elm327_pid_info_t *info = elm327_get_pid_info(pid);
                                            if (info) {
                                                printf("%-15s: %7.1f %-5s  ",
                                                       info->name, value, info->unit);
                                            }
                                            g_sample_count++;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                /* Sequential query: N round-trips */
                for (int i = 0; i < poll_count && !g_interrupt; i++) {
                    char cmd[16];
                    elm327_build_pid_query(poll_pids[i], cmd, sizeof(cmd));

                    char resp[256];
                    if ([g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)] &&
                        !elm327_is_error_response(resp)) {

                        uint8_t pid;
                        uint8_t data[ELM327_MAX_DATA_BYTES];
                        int data_len;

                        if (elm327_parse_pid_response(resp, &pid, data, &data_len)) {
                            double value = elm327_convert_pid_value(pid, data, data_len);
                            const elm327_pid_info_t *info = elm327_get_pid_info(pid);

                            if (info) {
                                printf("%-15s: %7.1f %-5s  ",
                                       info->name, value, info->unit);
                            }

                            g_sample_count++;
                        }
                    }
                }
            }

            fflush(stdout);
        }
    }

    printf("\n\n  Polling stopped. (samples: %u)\n",
           g_sample_count);
    g_interrupt = NO;
}

static void action_can_monitor(void) {
    if (!g_ble.isConnected || !g_ble.serviceDiscoveryDone) {
        printf("  Not connected.\n");
        return;
    }

    char resp[256];

    /* Passive init: CAN 500k, Silent Mode, raw formatting */
    [g_ble sendCommand:"ATSP6" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATCSM1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATS1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATCAF0" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATH1" response:resp maxLen:sizeof(resp)];

    const char *monitor_cmd = g_stn_detected ? "STMA" : "ATMA";
    printf("  Starting CAN bus monitor (%s, Silent Mode)...\n", monitor_cmd);
    printf("  Press Ctrl+C to stop.\n\n");
    printf("  %-8s %-5s %s\n", "CAN ID", "DLC", "Data");
    printf("  %-8s %-5s %s\n", "------", "---", "------------------------");

    g_interrupt = NO;
    g_ble->_canLineLen = 0;
    g_ble.canMonitorActive = YES;

    /* Clear response state and send monitor command */
    memset(g_ble->_responseBuf, 0, sizeof(g_ble->_responseBuf));
    g_ble->_responseLen = 0;
    g_ble->_responseReady = NO;

    char monitor_str[16];
    snprintf(monitor_str, sizeof(monitor_str), "%s\r", monitor_cmd);
    [g_ble sendRawString:monitor_str];

    /* Pump run loop until interrupted */
    while (!g_interrupt && g_ble.isConnected) {
        @autoreleasepool {
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                     beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.01]];
        }
    }

    /* Stop monitoring: send CR to interrupt */
    g_ble.canMonitorActive = NO;
    [g_ble sendRawString:"\r"];
    run_loop_for(0.2);

    printf("\n  CAN monitor stopped. (frames: %u)\n",
           g_can_frame_count);
    g_interrupt = NO;
}

static void action_disconnect(void) {
    if (!g_ble.isConnected) {
        printf("  Not connected.\n");
        return;
    }
    [g_ble disconnect];
    g_stn_detected = false;
    memset(&g_stn_info, 0, sizeof(g_stn_info));
    printf("  Disconnected.\n");
}

/*******************************************************************************
 * Menu
 ******************************************************************************/

static void print_menu(void) {
    printf("\n");
    printf("=== macOS OBD Reader ===\n");

    if (g_ble.isConnected) {
        printf("  Connected: %s", g_ble.connectedDeviceName.UTF8String ?: "unknown");
        if (g_stn_detected) {
            printf(" [STN: %s]", g_stn_info.device_id);
        }
        printf("\n");
    }

    printf("\n");
    printf("  1. Scan for devices\n");
    printf("  2. Connect to device\n");
    printf("  3. Initialize adapter (ELM327/STN)\n");
    printf("  4. Query supported PIDs\n");
    printf("  5. Poll PID (single)\n");
    printf("  6. Poll PIDs (continuous - motorsports basic)\n");
    printf("  7. Monitor CAN bus (%s)\n", g_stn_detected ? "STMA" : "ATMA");
    printf("  8. Send raw AT command\n");
    printf("  9. Disconnect\n");
    printf("  0. Quit\n");
    printf("\n");
    printf("  Select: ");
    fflush(stdout);
}

static void action_raw_command(void) {
    if (!g_ble.isConnected || !g_ble.serviceDiscoveryDone) {
        printf("  Not connected.\n");
        return;
    }

    printf("  Enter command: ");
    fflush(stdout);

    char input[128];
    if (!fgets(input, sizeof(input), stdin)) return;

    /* Strip newline */
    size_t len = strlen(input);
    while (len > 0 && (input[len - 1] == '\n' || input[len - 1] == '\r')) {
        input[--len] = '\0';
    }
    if (len == 0) return;

    char resp[512];
    printf("  >> %s\n", input);

    if ([g_ble sendCommand:input response:resp maxLen:sizeof(resp)]) {
        printf("  << %s\n", resp);
    } else {
        printf("  << (timeout)\n");
    }
}

/*******************************************************************************
 * CLI Mode
 ******************************************************************************/

/** Log to stderr (CLI mode) */
static void cli_log(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void cli_log(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "[INFO] ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

/** Escape a string for JSON output */
static void json_print_string(FILE *f, const char *s) {
    fputc('"', f);
    for (; *s; s++) {
        switch (*s) {
            case '"':  fputs("\\\"", f); break;
            case '\\': fputs("\\\\", f); break;
            case '\n': fputs("\\n", f); break;
            case '\r': fputs("\\r", f); break;
            case '\t': fputs("\\t", f); break;
            default:
                if ((unsigned char)*s < 0x20) {
                    fprintf(f, "\\u%04x", (unsigned char)*s);
                } else {
                    fputc(*s, f);
                }
                break;
        }
    }
    fputc('"', f);
}

/**
 * BLE scan + connect (shared by all CLI subcommands).
 * Scans, matches device by name prefix, connects, waits for service discovery.
 * Returns matched device index, or exits with code 1 on failure.
 */
static int cli_ble_connect(const char *device_prefix) {
    /* Wait for Bluetooth */
    cli_log("Waiting for Bluetooth...");
    if (![g_ble isBleReady]) {
        wait_for(&(g_ble->_bleReady), 5.0);
    }
    if (![g_ble isBleReady]) {
        fprintf(stderr, "[ERROR] Bluetooth not available\n");
        exit(1);
    }

    /* Scan */
    cli_log("Scanning for devices...");
    [g_ble startScan];
    run_loop_for(5.0);
    [g_ble stopScan];

    if (g_ble.discoveredNames.count == 0) {
        fprintf(stderr, "[ERROR] No OBD devices found\n");
        exit(1);
    }

    /* Match device by name prefix (case-insensitive) */
    NSString *prefix = [[NSString stringWithUTF8String:device_prefix] uppercaseString];
    int matchIdx = -1;
    for (int i = 0; i < (int)g_ble.discoveredNames.count; i++) {
        NSString *name = [g_ble.discoveredNames[i] uppercaseString];
        if ([name containsString:prefix]) {
            matchIdx = i;
            cli_log("Found: %s (RSSI: %d)", g_ble.discoveredNames[i].UTF8String,
                    g_ble.discoveredRSSIs[i].intValue);
            break;
        }
    }

    if (matchIdx < 0) {
        fprintf(stderr, "[ERROR] No device matching '%s' found. Available:\n", device_prefix);
        for (int i = 0; i < (int)g_ble.discoveredNames.count; i++) {
            fprintf(stderr, "  [%d] %s\n", i, g_ble.discoveredNames[i].UTF8String);
        }
        exit(1);
    }

    /* Connect */
    cli_log("Connecting...");
    if (![g_ble connectToDevice:matchIdx]) {
        fprintf(stderr, "[ERROR] Failed to initiate connection\n");
        exit(1);
    }

    /* Wait for service discovery */
    NSDate *timeout = [NSDate dateWithTimeIntervalSinceNow:10.0];
    while (!g_ble.serviceDiscoveryDone &&
           [[NSDate date] compare:timeout] == NSOrderedAscending) {
        @autoreleasepool {
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                     beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.05]];
        }
    }

    if (!g_ble.serviceDiscoveryDone) {
        fprintf(stderr, "[ERROR] Connection/service discovery timeout\n");
        exit(1);
    }

    return matchIdx;
}

/**
 * Standard OBD-II auto-connect: BLE connect + ELM327 init + STN detection.
 * Returns matched device index, or exits with code 1 on failure.
 */
static int cli_auto_connect(const char *device_prefix) {
    int matchIdx = cli_ble_connect(device_prefix);

    /* ELM327 init */
    cli_log("Initializing ELM327...");
    char cmds[ELM327_MAX_INIT_CMDS][ELM327_CMD_MAX_LEN];
    int cmd_count;
    elm327_build_init_cmds(cmds, &cmd_count);

    char resp[512];
    for (int i = 0; i < cmd_count; i++) {
        if ([g_ble sendCommand:cmds[i] response:resp maxLen:sizeof(resp)]) {
            if (strcmp(cmds[i], "ATZ") == 0) {
                run_loop_for(0.5);
            } else if (strcmp(cmds[i], "ATI") == 0) {
                cli_log("ELM327 initialized: %s", resp);
            }
        }
    }

    /* STN detection */
    char sti_resp[256];
    if ([g_ble sendCommand:"STI" response:sti_resp maxLen:sizeof(sti_resp)]) {
        if (stn2255_detect(resp, sti_resp, &g_stn_info)) {
            g_stn_detected = true;
            cli_log("STN detected: %s", g_stn_info.device_id);

            /* STDI for hardware ID */
            char stdi_resp[256];
            if ([g_ble sendCommand:"STDI" response:stdi_resp maxLen:sizeof(stdi_resp)]) {
                const char *s = stdi_resp;
                while (*s == ' ' || *s == '\r' || *s == '\n') s++;
                strncpy(g_stn_info.hardware_id, s, sizeof(g_stn_info.hardware_id) - 1);
            }

            /* STN optimization commands */
            char stn_cmds[STN2255_MAX_INIT_CMDS][STN2255_CMD_MAX_LEN];
            int stn_cmd_count;
            stn2255_build_init_cmds(stn_cmds, &stn_cmd_count);
            for (int i = 0; i < stn_cmd_count; i++) {
                char stn_resp[256];
                [g_ble sendCommand:stn_cmds[i] response:stn_resp maxLen:sizeof(stn_resp)];
            }
        }
    }

    return matchIdx;
}

/**
 * PT-CAN passive connect: BLE connect + minimal AT init + silent mode.
 * No OBD protocol negotiation — adapter becomes a passive CAN sniffer.
 * Returns matched device index, or exits with code 1 on failure.
 */
static int cli_ptcan_connect(const char *device_prefix, const uint32_t *filter_ids,
                              int filter_count) {
    int matchIdx = cli_ble_connect(device_prefix);
    char resp[512];

    /* Minimal init: reset + echo off + linefeeds off */
    cli_log("Initializing PT-CAN passive mode...");

    static const char *init_cmds[] = { "ATZ", "ATE0", "ATL0" };
    for (int i = 0; i < 3; i++) {
        if ([g_ble sendCommand:init_cmds[i] response:resp maxLen:sizeof(resp)]) {
            if (strcmp(init_cmds[i], "ATZ") == 0) {
                run_loop_for(0.5);
            }
        }
    }

    /* STN detection (for STFAP/STM support) */
    char sti_resp[256];
    if ([g_ble sendCommand:"STI" response:sti_resp maxLen:sizeof(sti_resp)]) {
        if (stn2255_detect(resp, sti_resp, &g_stn_info)) {
            g_stn_detected = true;
            cli_log("STN detected: %s", g_stn_info.device_id);
        }
    }

    /* Force protocol: ISO 15765-4 CAN 11-bit 500kbps */
    cli_log("Setting protocol: CAN 11-bit 500k (ATSP6)");
    if (![g_ble sendCommand:"ATSP6" response:resp maxLen:sizeof(resp)]) {
        fprintf(stderr, "[ERROR] ATSP6 failed\n");
        exit(1);
    }

    /* CAN Silent Monitoring — adapter will NOT send ACK on CAN bus */
    cli_log("Enabling silent mode (ATCSM1) — no ACK on CAN bus");
    if (![g_ble sendCommand:"ATCSM1" response:resp maxLen:sizeof(resp)]) {
        fprintf(stderr, "[WARN] ATCSM1 failed (may not be supported)\n");
    }

    /* Spaces ON for readable output */
    [g_ble sendCommand:"ATS1" response:resp maxLen:sizeof(resp)];

    /* Raw CAN formatting: no ISO-TP interpretation */
    [g_ble sendCommand:"ATCAF0" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATH1" response:resp maxLen:sizeof(resp)];

    /* Set hardware CAN filters if specified */
    if (filter_count > 0) {
        /* Clear existing filters first */
        if (g_stn_detected) {
            [g_ble sendCommand:"STFAC" response:resp maxLen:sizeof(resp)];
        }

        for (int i = 0; i < filter_count; i++) {
            char filter_cmd[64];
            if (g_stn_detected) {
                /* STN: STFAP with exact match mask */
                snprintf(filter_cmd, sizeof(filter_cmd), "STFAP %03X,7FF", filter_ids[i]);
            } else {
                /* ELM327: ATCF/ATCM pair */
                snprintf(filter_cmd, sizeof(filter_cmd), "ATCF %03X", filter_ids[i]);
                [g_ble sendCommand:filter_cmd response:resp maxLen:sizeof(resp)];
                snprintf(filter_cmd, sizeof(filter_cmd), "ATCM 7FF");
            }
            cli_log("Filter: %s", filter_cmd);
            [g_ble sendCommand:filter_cmd response:resp maxLen:sizeof(resp)];
        }
    }

    cli_log("PT-CAN passive mode ready (silent, no ACK)");
    return matchIdx;
}

/** CLI: scan - BLE scan and output JSON device list */
static int cli_scan(void) {
    /* Wait for Bluetooth */
    cli_log("Waiting for Bluetooth...");
    if (![g_ble isBleReady]) {
        wait_for(&(g_ble->_bleReady), 5.0);
    }
    if (![g_ble isBleReady]) {
        fprintf(stderr, "[ERROR] Bluetooth not available\n");
        return 1;
    }

    cli_log("Scanning for devices...");
    [g_ble startScan];
    run_loop_for(5.0);
    [g_ble stopScan];

    cli_log("Found %lu device(s)", (unsigned long)g_ble.discoveredNames.count);

    /* JSON output */
    printf("[");
    for (int i = 0; i < (int)g_ble.discoveredNames.count; i++) {
        if (i > 0) printf(",");
        OBDTransportType t = (OBDTransportType)[g_ble.discoveredTransports[i] intValue];
        printf("\n  {\"index\":%d,\"name\":", i);
        json_print_string(stdout, g_ble.discoveredNames[i].UTF8String);
        printf(",\"rssi\":%d,\"transport\":\"%s\"}",
               g_ble.discoveredRSSIs[i].intValue,
               t == OBD_TRANSPORT_CLASSIC ? "classic" : "ble");
    }
    if (g_ble.discoveredNames.count > 0) printf("\n");
    printf("]\n");

    return 0;
}

/** CLI: query <device> <pid_hex...> */
static int cli_query(const char *device, int pid_argc, const char *pid_argv[]) {
    if (pid_argc < 1) {
        fprintf(stderr, "[ERROR] Usage: query <device> <pid_hex> [pid_hex...]\n");
        return 1;
    }

    /* Parse PID hex values */
    uint8_t pids[STN2255_MAX_BATCH_PIDS];
    int pid_count = 0;
    for (int i = 0; i < pid_argc && pid_count < STN2255_MAX_BATCH_PIDS; i++) {
        unsigned int val;
        if (sscanf(pid_argv[i], "%x", &val) == 1 && val <= 0xFF) {
            pids[pid_count++] = (uint8_t)val;
        } else {
            fprintf(stderr, "[ERROR] Invalid PID hex: %s\n", pid_argv[i]);
            return 1;
        }
    }

    int matchIdx = cli_auto_connect(device);

    cli_log("Querying PIDs: %s%s%s",
            pid_argc >= 1 ? pid_argv[0] : "",
            pid_argc >= 2 ? " " : "", pid_argc >= 2 ? pid_argv[1] : "");

    /* Build JSON output */
    printf("{\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"stn\":%s,\"results\":[\n", g_stn_detected ? "true" : "false");

    int result_count = 0;

    if (g_stn_detected && pid_count > 1) {
        /* Batch query */
        char cmd[64];
        stn2255_build_batch_query(pids, pid_count, cmd, sizeof(cmd));
        char resp[512];

        if ([g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)] &&
            !elm327_is_error_response(resp)) {

            stn2255_batch_result_t batch;
            stn2255_parse_batch_response(resp, &batch);

            for (int i = 0; i < batch.count; i++) {
                if (!batch.results[i].valid) continue;
                if (result_count > 0) printf(",\n");

                const elm327_pid_info_t *info = elm327_get_pid_info(batch.results[i].pid);
                printf("  {\"pid\":\"%02X\",\"name\":", batch.results[i].pid);
                json_print_string(stdout, info ? info->name : "Unknown");
                printf(",\"value\":%.2f,\"unit\":", batch.results[i].value);
                json_print_string(stdout, info ? info->unit : "");

                /* Raw hex */
                printf(",\"raw\":\"");
                for (int j = 0; j < batch.results[i].data_len; j++) {
                    if (j > 0) printf(" ");
                    printf("%02X", batch.results[i].data[j]);
                }
                printf("\"}");
                result_count++;
            }

            /* Fallback: poll PIDs missing from batch */
            for (int i = 0; i < pid_count; i++) {
                bool found = false;
                for (int j = 0; j < batch.count; j++) {
                    if (batch.results[j].pid == pids[i] && batch.results[j].valid) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    char single_cmd[16];
                    elm327_build_pid_query(pids[i], single_cmd, sizeof(single_cmd));
                    char single_resp[256];
                    if ([g_ble sendCommand:single_cmd response:single_resp maxLen:sizeof(single_resp)] &&
                        !elm327_is_error_response(single_resp)) {
                        uint8_t pid;
                        uint8_t data[ELM327_MAX_DATA_BYTES];
                        int data_len;
                        if (elm327_parse_pid_response(single_resp, &pid, data, &data_len)) {
                            double value = elm327_convert_pid_value(pid, data, data_len);
                            const elm327_pid_info_t *info = elm327_get_pid_info(pid);
                            if (result_count > 0) printf(",\n");
                            printf("  {\"pid\":\"%02X\",\"name\":", pid);
                            json_print_string(stdout, info ? info->name : "Unknown");
                            printf(",\"value\":%.2f,\"unit\":", value);
                            json_print_string(stdout, info ? info->unit : "");
                            printf(",\"raw\":\"");
                            for (int j = 0; j < data_len; j++) {
                                if (j > 0) printf(" ");
                                printf("%02X", data[j]);
                            }
                            printf("\"}");
                            result_count++;
                        }
                    }
                }
            }
        } else {
            fprintf(stderr, "[WARN] Batch query failed, falling back to sequential\n");
            goto sequential;
        }
    } else {
sequential:
        /* Sequential query */
        for (int i = 0; i < pid_count; i++) {
            char cmd[16];
            elm327_build_pid_query(pids[i], cmd, sizeof(cmd));
            char resp[256];

            if ([g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)] &&
                !elm327_is_error_response(resp)) {
                uint8_t pid;
                uint8_t data[ELM327_MAX_DATA_BYTES];
                int data_len;

                if (elm327_parse_pid_response(resp, &pid, data, &data_len)) {
                    double value = elm327_convert_pid_value(pid, data, data_len);
                    const elm327_pid_info_t *info = elm327_get_pid_info(pid);

                    if (result_count > 0) printf(",\n");
                    printf("  {\"pid\":\"%02X\",\"name\":", pid);
                    json_print_string(stdout, info ? info->name : "Unknown");
                    printf(",\"value\":%.2f,\"unit\":", value);
                    json_print_string(stdout, info ? info->unit : "");
                    printf(",\"raw\":\"");
                    for (int j = 0; j < data_len; j++) {
                        if (j > 0) printf(" ");
                        printf("%02X", data[j]);
                    }
                    printf("\"}");
                    result_count++;
                }
            } else {
                cli_log("PID %02X: no data or error", pids[i]);
            }
        }
    }

    printf("\n]}\n");

    [g_ble disconnect];
    return 0;
}

/** CLI: supported <device> */
static int cli_supported(const char *device) {
    int matchIdx = cli_auto_connect(device);

    bool supported[256];
    memset(supported, 0, sizeof(supported));

    cli_log("Querying supported PIDs...");

    static const uint8_t query_pids[] = { 0x00, 0x20, 0x40, 0x60 };
    for (int q = 0; q < 4; q++) {
        char cmd[16];
        elm327_build_pid_query(query_pids[q], cmd, sizeof(cmd));
        char resp[256];

        if ([g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)]) {
            if (!elm327_is_error_response(resp)) {
                elm327_parse_supported_pids(resp, query_pids[q], supported);
            } else {
                break;
            }
        } else {
            break;
        }
    }

    /* JSON output */
    printf("{\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"supported\":[\n");

    int count = 0;
    for (int i = 1; i < 256; i++) {
        if (!supported[i]) continue;
        const elm327_pid_info_t *info = elm327_get_pid_info(i);
        if (count > 0) printf(",\n");
        printf("  {\"pid\":\"%02X\",\"name\":", i);
        json_print_string(stdout, info ? info->name : "Unknown");
        printf(",\"unit\":");
        json_print_string(stdout, info ? info->unit : "");
        printf("}");
        count++;
    }

    printf("\n]}\n");
    cli_log("Total: %d supported PIDs", count);

    [g_ble disconnect];
    return 0;
}

/**
 * CLI: monitor <device> [seconds] [CAN_IDs...]
 * Passive CAN monitor (silent mode — no ACK on bus).
 * Optional CAN ID filters via STFAP.
 */
static int cli_monitor(const char *device, int seconds,
                        const char *filter_argv[], int filter_argc) {
    /* Parse filter CAN IDs */
    uint32_t filter_ids[16];
    int filter_count = 0;
    for (int i = 0; i < filter_argc && filter_count < 16; i++) {
        unsigned int val;
        if (sscanf(filter_argv[i], "%x", &val) == 1 && val <= 0x7FF) {
            filter_ids[filter_count++] = val;
        } else {
            fprintf(stderr, "[ERROR] Invalid CAN ID: %s (must be 000-7FF hex)\n", filter_argv[i]);
            return 1;
        }
    }

    int matchIdx = cli_ptcan_connect(device, filter_ids, filter_count);

    const char *monitor_cmd = g_stn_detected ? "STMA" : "ATMA";
    cli_log("Starting CAN monitor (%s, silent) for %d seconds...", monitor_cmd, seconds);
    if (filter_count > 0) {
        fprintf(stderr, "[INFO] Filters: ");
        for (int i = 0; i < filter_count; i++) {
            fprintf(stderr, "%s%03X", i > 0 ? " " : "", filter_ids[i]);
        }
        fprintf(stderr, "\n");
    }

    g_ble->_canLineLen = 0;
    g_ble.canMonitorActive = YES;
    g_cli_can_frame_count = 0;

    /* Clear response state */
    memset(g_ble->_responseBuf, 0, sizeof(g_ble->_responseBuf));
    g_ble->_responseLen = 0;
    g_ble->_responseReady = NO;

    /* Send monitor command */
    char monitor_str[16];
    snprintf(monitor_str, sizeof(monitor_str), "%s\r", monitor_cmd);
    [g_ble sendRawString:monitor_str];

    /* Collect for specified duration */
    NSDate *endTime = [NSDate dateWithTimeIntervalSinceNow:seconds];

    while ([[NSDate date] compare:endTime] == NSOrderedAscending &&
           g_ble.isConnected && !g_interrupt) {
        @autoreleasepool {
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                     beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.01]];
        }
    }

    /* Stop monitoring */
    g_ble.canMonitorActive = NO;
    [g_ble sendRawString:"\r"];
    run_loop_for(0.2);

    cli_log("CAN monitor stopped. Frames: %d", g_cli_can_frame_count);

    /* JSON output */
    printf("{\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"mode\":\"silent\",\"duration_s\":%d", seconds);
    if (filter_count > 0) {
        printf(",\"filters\":[");
        for (int i = 0; i < filter_count; i++) {
            if (i > 0) printf(",");
            printf("\"%03X\"", filter_ids[i]);
        }
        printf("]");
    }
    printf(",\"frames\":[\n");

    for (int i = 0; i < g_cli_can_frame_count; i++) {
        if (i > 0) printf(",\n");
        printf("  {\"id\":\"%03X\",\"dlc\":%d,\"data\":\"",
               g_cli_can_frames[i].can_id, g_cli_can_frames[i].dlc);
        for (int j = 0; j < g_cli_can_frames[i].dlc; j++) {
            if (j > 0) printf(" ");
            printf("%02X", g_cli_can_frames[i].data[j]);
        }
        printf("\"}");
    }

    if (g_cli_can_frame_count > 0) printf("\n");
    printf("]}\n");

    [g_ble disconnect];
    g_interrupt = NO;
    return 0;
}

/**
 * CLI: detect <device>
 * Auto-detect D-CAN (OBD port) vs PT-CAN (direct bus tap).
 *
 * 3-state detection:
 *   1. Silent mode passive listen 3 seconds
 *   2. If frames received → "ptcan" (confirmed)
 *   3. If no frames → probe query (0100) to verify D-CAN
 *   4. If probe response → "dcan" (confirmed)
 *   5. If no probe response → "unknown" (IGN OFF or not connected)
 */
static int cli_detect(const char *device) {
    int matchIdx = cli_ble_connect(device);
    char resp[512];

    /* Minimal init: reset + echo off + linefeeds off */
    cli_log("Initializing for bus detection...");
    static const char *init_cmds[] = { "ATZ", "ATE0", "ATL0" };
    for (int i = 0; i < 3; i++) {
        if ([g_ble sendCommand:init_cmds[i] response:resp maxLen:sizeof(resp)]) {
            if (strcmp(init_cmds[i], "ATZ") == 0) {
                run_loop_for(0.5);
            }
        }
    }

    /* STN detection */
    char sti_resp[256];
    if ([g_ble sendCommand:"STI" response:sti_resp maxLen:sizeof(sti_resp)]) {
        if (stn2255_detect(resp, sti_resp, &g_stn_info)) {
            g_stn_detected = true;
            cli_log("STN detected: %s", g_stn_info.device_id);
        }
    }

    /* Battery voltage via ATRV */
    double voltage = 0.0;
    cli_log("Checking battery voltage (ATRV)...");
    if ([g_ble sendCommand:"ATRV" response:resp maxLen:sizeof(resp)]) {
        elm327_parse_voltage(resp, &voltage);
        cli_log("Battery voltage: %.1fV", voltage);
    }

    /* === Phase 1: Passive listen (3 seconds) === */
    cli_log("Phase 1: Passive listening for 3 seconds...");

    /* Force CAN 500k + Silent Mode + raw formatting */
    [g_ble sendCommand:"ATSP6" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATCSM1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATS1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATCAF0" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATH1" response:resp maxLen:sizeof(resp)];

    g_ble->_canLineLen = 0;
    g_ble.canMonitorActive = YES;
    g_cli_can_frame_count = 0;

    memset(g_ble->_responseBuf, 0, sizeof(g_ble->_responseBuf));
    g_ble->_responseLen = 0;
    g_ble->_responseReady = NO;

    const char *monitor_cmd = g_stn_detected ? "STMA" : "ATMA";
    char monitor_str[16];
    snprintf(monitor_str, sizeof(monitor_str), "%s\r", monitor_cmd);
    [g_ble sendRawString:monitor_str];

    NSDate *endTime = [NSDate dateWithTimeIntervalSinceNow:3.0];
    while ([[NSDate date] compare:endTime] == NSOrderedAscending &&
           g_ble.isConnected && !g_interrupt) {
        @autoreleasepool {
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                     beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.01]];
        }
    }

    /* Stop monitoring */
    g_ble.canMonitorActive = NO;
    [g_ble sendRawString:"\r"];
    run_loop_for(0.2);

    int frames = g_cli_can_frame_count;
    const char *mode;
    bool probe_responded = false;

    if (frames > 0) {
        cli_log("Phase 1 result: CAN frames detected (%d)", frames);
    } else {
        cli_log("Phase 1 result: No CAN frames");
    }

    /* === Phase 2: Probe query (always run) ===
     * 0100 is a read-only diagnostic request, safe on any bus.
     * Determines whether standard OBD-II is available regardless of
     * broadcast traffic detected in Phase 1. */
    cli_log("Phase 2: Probing with 0100...");

    /* Switch to active mode for probe */
    [g_ble sendCommand:"ATCSM0" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATSP0" response:resp maxLen:sizeof(resp)];

    char probe_resp[256];
    if ([g_ble sendCommand:"0100" response:probe_resp maxLen:sizeof(probe_resp)] &&
        !elm327_is_error_response(probe_resp)) {
        probe_responded = true;
        cli_log("Phase 2 result: ECU responded");
    } else {
        cli_log("Phase 2 result: No ECU response");
    }

    /*  Phase 1 (frames)  |  Phase 2 (probe)  |  Result
     *  ------------------+-------------------+------------------
     *  YES               |  YES              |  dcan (broadcast)
     *  YES               |  NO               |  ptcan
     *  NO                |  YES              |  dcan
     *  NO                |  NO               |  unknown
     */
    if (probe_responded) {
        mode = "dcan";
        cli_log("Result: D-CAN (OBD-II port%s)", frames > 0 ? ", with broadcast" : "");
    } else if (frames > 0) {
        mode = "ptcan";
        cli_log("Result: PT-CAN (direct bus tap)");
    } else {
        mode = "unknown";
        cli_log("Result: undetermined (IGN OFF?)");
    }

    /* Count unique CAN IDs */
    int unique_ids = 0;
    uint32_t seen_ids[512];
    for (int i = 0; i < frames && unique_ids < 512; i++) {
        bool found = false;
        for (int j = 0; j < unique_ids; j++) {
            if (seen_ids[j] == g_cli_can_frames[i].can_id) {
                found = true;
                break;
            }
        }
        if (!found) {
            seen_ids[unique_ids++] = g_cli_can_frames[i].can_id;
        }
    }

    /* JSON output */
    printf("{\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"mode\":");
    json_print_string(stdout, mode);
    printf(",\"voltage\":%.1f", voltage);
    printf(",\"broadcast\":%s", (frames > 0) ? "true" : "false");
    printf(",\"frames_received\":%d", frames);
    printf(",\"probe_responded\":%s", probe_responded ? "true" : "false");
    printf(",\"unique_can_ids\":%d", unique_ids);
    if (unique_ids > 0) {
        printf(",\"can_ids\":[");
        for (int i = 0; i < unique_ids; i++) {
            if (i > 0) printf(",");
            printf("\"%03X\"", seen_ids[i]);
        }
        printf("]");
    }
    printf("}\n");

    [g_ble disconnect];
    g_interrupt = NO;
    return 0;
}

/** CLI: raw <device> <command> */
static int cli_raw(const char *device, const char *command) {
    cli_auto_connect(device);

    cli_log("Sending: %s", command);

    char resp[512];
    if ([g_ble sendCommand:command response:resp maxLen:sizeof(resp)]) {
        printf("%s\n", resp);
    } else {
        fprintf(stderr, "[ERROR] Command timeout\n");
        [g_ble disconnect];
        return 1;
    }

    [g_ble disconnect];
    return 0;
}

/*******************************************************************************
 * CLI: discover <device>
 * Interactive PT-CAN signal auto-discovery.
 * Guides user through capture phases with skip/confirm for each signal.
 ******************************************************************************/

/** Phase configuration */
static const struct {
    const char *name;
    int         duration_s;
    const char *instruction;
    const char *warning;        /**< NULL if no special warning */
} s_disc_phases[DISC_NUM_PHASES] = {
    { "Baseline",     5,
      "Keep engine running. Do NOT touch any controls.", NULL },
    { "Steering",     8,
      "Turn steering wheel to FULL LEFT, then FULL RIGHT (lock to lock). Repeat 2-3 times.",
      NULL },
    { "Throttle",     8,
      "Press accelerator pedal ALL THE WAY down, then release. Repeat 2-3 times.",
      NULL },
    { "Brake",        8,
      "Press brake pedal FIRMLY all the way down, then release. Repeat 2-3 times.",
      NULL },
    { "Gear",        10,
      "Shift through P -> R -> N -> D (hold brake, wait 1s per gear).",
      "Manual transmission vehicles may not broadcast gear position." },
    { "Wheel Speed", 15,
      "Drive slowly (30+ km/h) — include a turn if possible, then stop.",
      "Requires driving. ABS/ESC may be on a separate CAN bus." },
};

/** Read a single char from stdin while pumping NSRunLoop for BLE. */
static int disc_read_char(void) {
    int ch = 0;
    while (ch == 0 && g_ble.isConnected) {
        @autoreleasepool {
            fd_set fds;
            FD_ZERO(&fds);
            FD_SET(STDIN_FILENO, &fds);
            struct timeval tv = { .tv_sec = 0, .tv_usec = 50000 };
            if (select(STDIN_FILENO + 1, &fds, NULL, NULL, &tv) > 0) {
                ch = fgetc(stdin);
            }
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                     beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.01]];
        }
    }
    return ch;
}

/** Capture one phase. Returns false if connection lost. */
static bool disc_capture_phase(disc_phase_t *phase, int duration_s,
                                 const char *monitor_cmd) {
    g_ble->_canLineLen = 0;
    g_ble.canMonitorActive = YES;
    g_cli_can_frame_count = 0;

    memset(g_ble->_responseBuf, 0, sizeof(g_ble->_responseBuf));
    g_ble->_responseLen = 0;
    g_ble->_responseReady = NO;

    char mon_str[16];
    snprintf(mon_str, sizeof(mon_str), "%s\r", monitor_cmd);
    [g_ble sendRawString:mon_str];

    int64_t start_ms = get_timestamp_ms();

    NSDate *endTime = [NSDate dateWithTimeIntervalSinceNow:duration_s];
    while ([[NSDate date] compare:endTime] == NSOrderedAscending &&
           g_ble.isConnected && !g_interrupt) {
        @autoreleasepool {
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                     beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.01]];
        }
    }

    int64_t end_ms = get_timestamp_ms();

    g_ble.canMonitorActive = NO;
    [g_ble sendRawString:"\r"];
    run_loop_for(0.2);

    for (int i = 0; i < g_cli_can_frame_count; i++) {
        disc_phase_add_frame(phase,
                              g_cli_can_frames[i].can_id,
                              g_cli_can_frames[i].data,
                              g_cli_can_frames[i].dlc);
    }
    disc_phase_finalize(phase, (double)(end_ms - start_ms) / 1000.0);

    return g_ble.isConnected;
}

/** Print a candidate result line to stderr */
static void disc_print_candidate(const char *name, const disc_candidate_t *c) {
    fprintf(stderr, "    %-12s 0x%03X [%d] @ %.1f Hz  byte %d",
            name, c->can_id, c->dlc, c->hz, c->byte_idx);
    if (c->byte2_idx >= 0) fprintf(stderr, ",%d", c->byte2_idx);
    fprintf(stderr, "  score=%.1f  confidence=%s\n",
            c->score, disc_confidence_name(c->confidence));

    /* Characterization details (if available) */
    if (c->signedness != DISC_SIGN_UNKNOWN) {
        fprintf(stderr, "                 ");
        if (c->byte2_idx >= 0)
            fprintf(stderr, "%s-endian ", disc_endian_name(c->endianness));
        fprintf(stderr, "%s  raw=[%d..%d]\n",
                disc_sign_name(c->signedness), c->raw_min, c->raw_max);
    }
}

static int cli_discover(const char *device) {
    cli_ptcan_connect(device, NULL, 0);

    const char *monitor_cmd = g_stn_detected ? "STMA" : "ATMA";

    disc_phase_t baseline;
    disc_phase_init(&baseline);

    /* Exclusion list — grows as signals are confirmed */
    uint32_t excl[DISC_SIG_COUNT];
    int excl_count = 0;

    /* Confirmed results */
    disc_result_t result;
    memset(&result, 0, sizeof(result));
    for (int i = 0; i < DISC_SIG_COUNT; i++) {
        result.signals[i].byte_idx = -1;
        result.signals[i].byte2_idx = -1;
    }

    bool skipped[DISC_NUM_PHASES] = {false};

    /* === Phase 1: Baseline (mandatory) === */
    fprintf(stderr, "\n=== Phase 1/%d: Baseline ===\n", DISC_NUM_PHASES);
    fprintf(stderr, "  %s\n", s_disc_phases[0].instruction);
    fprintf(stderr, "  Press Enter when ready...");
    disc_read_char();

    if (!g_ble.isConnected) goto disconnected;

    fprintf(stderr, "  Capturing %d seconds...", s_disc_phases[0].duration_s);
    fflush(stderr);
    if (!disc_capture_phase(&baseline, s_disc_phases[0].duration_s, monitor_cmd))
        goto disconnected;
    fprintf(stderr, " %d frames, %d CAN IDs\n",
            baseline.total_frames, baseline.id_count);

    /* === Active phases (2..N) with skip/confirm === */
    for (int p = 1; p < DISC_NUM_PHASES; p++) {
        fprintf(stderr, "\n=== Phase %d/%d: %s ===\n",
                p + 1, DISC_NUM_PHASES, s_disc_phases[p].name);
        fprintf(stderr, "  %s\n", s_disc_phases[p].instruction);
        if (s_disc_phases[p].warning)
            fprintf(stderr, "  NOTE: %s\n", s_disc_phases[p].warning);
        fprintf(stderr, "  [Enter] proceed  [s] skip > ");
        fflush(stderr);

        int ch = disc_read_char();
        if (!g_ble.isConnected) goto disconnected;
        if (ch == 's' || ch == 'S') {
            /* Consume trailing newline if any */
            fprintf(stderr, "  Skipped.\n");
            skipped[p] = true;
            continue;
        }

        /* Capture */
        disc_phase_t active;
        disc_phase_init(&active);

        fprintf(stderr, "  Capturing %d seconds...", s_disc_phases[p].duration_s);
        fflush(stderr);
        if (!disc_capture_phase(&active, s_disc_phases[p].duration_s, monitor_cmd))
            goto disconnected;
        fprintf(stderr, " %d frames, %d CAN IDs\n",
                active.total_frames, active.id_count);

        /* Analyze this phase */
        disc_signal_t sig;
        if      (p == DISC_PHASE_STEERING)    sig = DISC_SIG_STEERING;
        else if (p == DISC_PHASE_THROTTLE)    sig = DISC_SIG_THROTTLE;
        else if (p == DISC_PHASE_BRAKE)       sig = DISC_SIG_BRAKE;
        else if (p == DISC_PHASE_GEAR)        sig = DISC_SIG_GEAR;
        else if (p == DISC_PHASE_WHEEL_SPEED) sig = DISC_SIG_WHEEL_SPEED;
        else continue;

        disc_candidate_t cand = {0};
        cand.byte_idx = -1;
        cand.byte2_idx = -1;

        /* For throttle phase: also gets RPM */
        disc_candidate_t rpm_cand = {0};
        rpm_cand.byte_idx = -1;
        rpm_cand.byte2_idx = -1;
        bool rpm_det = false;

        bool found = disc_classify(&baseline, &active, sig,
                                    excl, excl_count, &cand,
                                    &rpm_cand, &rpm_det);

        /* Characterize detected signals */
        if (found) disc_characterize(&active, &cand);
        if (rpm_det) disc_characterize(&active, &rpm_cand);

        if (sig == DISC_SIG_THROTTLE) {
            /* Show RPM result first */
            if (rpm_det) {
                disc_print_candidate("rpm", &rpm_cand);
                fprintf(stderr, "  Include rpm? [y/n] > ");
                fflush(stderr);
                int c2 = disc_read_char();
                if (!g_ble.isConnected) goto disconnected;
                if (c2 == 'y' || c2 == 'Y' || c2 == '\n') {
                    result.signals[DISC_SIG_RPM] = rpm_cand;
                    result.found[DISC_SIG_RPM] = true;
                    excl[excl_count++] = rpm_cand.can_id;
                    fprintf(stderr, "  -> rpm included.\n");
                } else {
                    fprintf(stderr, "  -> rpm excluded.\n");
                }
            } else {
                fprintf(stderr, "    rpm          not detected\n");
            }

            /* Show throttle result */
            if (found) {
                disc_print_candidate("throttle", &cand);
                fprintf(stderr, "  Include throttle? [y/n] > ");
                fflush(stderr);
                int c2 = disc_read_char();
                if (!g_ble.isConnected) goto disconnected;
                if (c2 == 'y' || c2 == 'Y' || c2 == '\n') {
                    result.signals[DISC_SIG_THROTTLE] = cand;
                    result.found[DISC_SIG_THROTTLE] = true;
                    excl[excl_count++] = cand.can_id;
                    fprintf(stderr, "  -> throttle included.\n");
                } else {
                    fprintf(stderr, "  -> throttle excluded.\n");
                }
            } else {
                fprintf(stderr, "    throttle     not detected\n");
            }
        } else {
            /* Single signal phase */
            const char *sname = disc_signal_name(sig);
            if (found) {
                disc_print_candidate(sname, &cand);
                fprintf(stderr, "  Include %s? [y/n] > ", sname);
                fflush(stderr);
                int c2 = disc_read_char();
                if (!g_ble.isConnected) goto disconnected;
                if (c2 == 'y' || c2 == 'Y' || c2 == '\n') {
                    result.signals[sig] = cand;
                    result.found[sig] = true;
                    excl[excl_count++] = cand.can_id;
                    fprintf(stderr, "  -> %s included.\n", sname);
                } else {
                    fprintf(stderr, "  -> %s excluded.\n", sname);
                }
            } else {
                fprintf(stderr, "    %-12s not detected\n", sname);
            }
        }
    }

    /* JSON output (stdout) — only confirmed signals */
    printf("{\n");
    bool first = true;
    for (int s = 0; s < DISC_SIG_COUNT; s++) {
        if (!result.found[s]) continue;
        if (!first) printf(",\n");
        first = false;

        const disc_candidate_t *c = &result.signals[s];
        printf("  \"%s\":{\"can_id\":\"0x%03X\",\"dlc\":%d,\"hz\":%.1f",
               disc_signal_name((disc_signal_t)s), c->can_id, c->dlc, c->hz);
        if (c->byte_idx >= 0)
            printf(",\"byte\":%d", c->byte_idx);
        if (c->byte2_idx >= 0)
            printf(",\"byte2\":%d", c->byte2_idx);
        printf(",\"score\":%.1f,\"confidence\":\"%s\"",
               c->score, disc_confidence_name(c->confidence));
        if (c->byte2_idx >= 0 && c->endianness != DISC_ENDIAN_UNKNOWN)
            printf(",\"endian\":\"%s\"", disc_endian_name(c->endianness));
        if (c->signedness != DISC_SIGN_UNKNOWN)
            printf(",\"signed\":%s", c->signedness == DISC_SIGN_SIGNED ? "true" : "false");
        if (c->signedness != DISC_SIGN_UNKNOWN)
            printf(",\"raw_min\":%d,\"raw_max\":%d", c->raw_min, c->raw_max);
        printf("}");
    }
    if (!first) printf("\n");
    printf("}\n");

    /* Summary to stderr */
    fprintf(stderr, "\n=== Final Results ===\n");
    for (int s = 0; s < DISC_SIG_COUNT; s++) {
        if (result.found[s]) {
            disc_print_candidate(disc_signal_name((disc_signal_t)s),
                                  &result.signals[s]);
        }
    }

    [g_ble disconnect];
    g_interrupt = NO;
    return 0;

disconnected:
    fprintf(stderr, "\n[ERROR] Connection lost during discover\n");
    return 1;
}

static void cli_print_usage(void) {
    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "  macos_obd_reader                                     Interactive mode\n");
    fprintf(stderr, "  macos_obd_reader scan                                Scan for BLE OBD devices\n");
    fprintf(stderr, "  macos_obd_reader query <device> <PIDs..>             Query OBD-II PIDs\n");
    fprintf(stderr, "  macos_obd_reader supported <device>                  List supported PIDs\n");
    fprintf(stderr, "  macos_obd_reader monitor <device> [secs] [CAN_IDs]  CAN passive monitor\n");
    fprintf(stderr, "  macos_obd_reader raw <device> <command>              Send raw AT command\n");
    fprintf(stderr, "  macos_obd_reader detect <device>                     Auto-detect D-CAN vs PT-CAN\n");
    fprintf(stderr, "  macos_obd_reader discover <device>                   Auto-discover PT-CAN signals\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "  <device>:   BLE name prefix match (e.g., OBDLink, vLinker)\n");
    fprintf(stderr, "  <CAN_IDs>:  Optional hex filter IDs (e.g., 0A5 1A0 316)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "  detect:    Passive listen 3s to determine bus type (D-CAN/PT-CAN)\n");
    fprintf(stderr, "  discover:  Interactive guided signal discovery (steering/RPM/throttle/brake/gear)\n");
    fprintf(stderr, "  monitor:   Silent mode (no ACK) — safe for direct CAN bus tap\n");
    fprintf(stderr, "  query/supported/raw: Active mode (sends OBD-II requests)\n");
}

/** CLI mode entry point */
static int cli_main(int argc, const char *argv[]) {
    g_cli_mode = true;
    const char *subcmd = argv[1];

    /* Initialize dependencies */
    kDeviceNamePrefixes = @[
        @"OBDLink", @"vLinker", @"VLINKER", @"VLinker",
        @"ELM327", @"OBD", @"iOBD"
    ];

    signal(SIGINT, signal_handler);

    g_ble = [[BLEManager alloc] init];

    int ret = 0;

    if (strcmp(subcmd, "scan") == 0) {
        ret = cli_scan();
    } else if (strcmp(subcmd, "query") == 0) {
        if (argc < 4) {
            fprintf(stderr, "[ERROR] Usage: query <device> <pid_hex> [pid_hex...]\n");
            ret = 1;
        } else {
            ret = cli_query(argv[2], argc - 3, &argv[3]);
        }
    } else if (strcmp(subcmd, "supported") == 0) {
        if (argc < 3) {
            fprintf(stderr, "[ERROR] Usage: supported <device>\n");
            ret = 1;
        } else {
            ret = cli_supported(argv[2]);
        }
    } else if (strcmp(subcmd, "monitor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "[ERROR] Usage: monitor <device> [seconds] [CAN_ID_hex...]\n");
            ret = 1;
        } else {
            int secs = 5;
            int filter_start = 3;  /* argv index where filter IDs begin */
            /* If argv[3] looks like a number (seconds), consume it */
            if (argc >= 4) {
                char *endp;
                long val = strtol(argv[3], &endp, 10);
                if (*endp == '\0' && val > 0) {
                    secs = (int)val;
                    filter_start = 4;
                }
            }
            int filter_argc = argc - filter_start;
            const char **filter_argv = (filter_argc > 0) ? &argv[filter_start] : NULL;
            ret = cli_monitor(argv[2], secs, filter_argv, filter_argc);
        }
    } else if (strcmp(subcmd, "raw") == 0) {
        if (argc < 4) {
            fprintf(stderr, "[ERROR] Usage: raw <device> <command>\n");
            ret = 1;
        } else {
            ret = cli_raw(argv[2], argv[3]);
        }
    } else if (strcmp(subcmd, "detect") == 0) {
        if (argc < 3) {
            fprintf(stderr, "[ERROR] Usage: detect <device>\n");
            ret = 1;
        } else {
            ret = cli_detect(argv[2]);
        }
    } else if (strcmp(subcmd, "discover") == 0) {
        if (argc < 3) {
            fprintf(stderr, "[ERROR] Usage: discover <device>\n");
            ret = 1;
        } else {
            ret = cli_discover(argv[2]);
        }
    } else if (strcmp(subcmd, "-h") == 0 || strcmp(subcmd, "--help") == 0 ||
               strcmp(subcmd, "help") == 0) {
        cli_print_usage();
        ret = 0;
    } else {
        fprintf(stderr, "[ERROR] Unknown command: %s\n", subcmd);
        cli_print_usage();
        ret = 1;
    }

    return ret;
}

/*******************************************************************************
 * Main
 ******************************************************************************/

int main(int argc, const char *argv[]) {

    @autoreleasepool {
        /* CLI mode: if any arguments given, run non-interactive */
        if (argc >= 2) {
            return cli_main(argc, argv);
        }

        /* Interactive mode (original behavior) */
        printf("=========================================\n");
        printf("  CAN Reader\n");
        printf("=========================================\n\n");

        /* Initialize known device name prefixes */
        kDeviceNamePrefixes = @[
            @"OBDLink", @"vLinker", @"VLINKER", @"VLinker",
            @"ELM327", @"OBD", @"iOBD"
        ];

        /* Setup signal handler for Ctrl+C */
        signal(SIGINT, signal_handler);

        /* Initialize BLE manager */
        g_ble = [[BLEManager alloc] init];

        /* Wait for Bluetooth to power on */
        printf("  Waiting for Bluetooth...\n");
        if (![g_ble isBleReady]) {
            wait_for(&(g_ble->_bleReady), 5.0);
        }

        if (![g_ble isBleReady]) {
            printf("  Bluetooth initialization timeout.\n");
            printf("  Make sure Bluetooth is enabled in System Settings.\n");
        }

        /* Main menu loop */
        while (g_running) {
            @autoreleasepool {
                print_menu();

                /* Read input while pumping run loop */
                char input[16] = {0};

                /* Use select() to check for stdin while running the event loop */
                BOOL gotInput = NO;
                while (!gotInput && g_running) {
                    @autoreleasepool {
                        /* Pump the run loop briefly */
                        [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                                beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.05]];

                        /* Check if stdin has data */
                        fd_set fds;
                        FD_ZERO(&fds);
                        FD_SET(STDIN_FILENO, &fds);
                        struct timeval tv = { .tv_sec = 0, .tv_usec = 0 };

                        if (select(STDIN_FILENO + 1, &fds, NULL, NULL, &tv) > 0) {
                            if (fgets(input, sizeof(input), stdin)) {
                                gotInput = YES;
                            }
                        }
                    }
                }

                if (!g_running) break;

                int choice = atoi(input);

                switch (choice) {
                    case 1: action_scan(); break;
                    case 2: action_connect(); break;
                    case 3: action_init_elm327(); break;
                    case 4: action_query_supported_pids(); break;
                    case 5: action_poll_single_pid(); break;
                    case 6: action_poll_continuous(); break;
                    case 7: action_can_monitor(); break;
                    case 8: action_raw_command(); break;
                    case 9: action_disconnect(); break;
                    case 0:
                        g_running = NO;
                        break;
                    default:
                        printf("  Invalid selection.\n");
                        break;
                }
            }
        }

        /* Cleanup */
        [g_ble disconnect];

        printf("\nGoodbye.\n");
    }

    return 0;
}
