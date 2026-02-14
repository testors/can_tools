/**
 * @file main.m
 * @brief macOS PT-CAN Signal Auto-Discovery
 *
 * Standalone tool for interactive PT-CAN signal discovery.
 * Connects via BLE/Classic BT to an ELM327/STN adapter in passive mode,
 * guides user through 5 capture phases, and identifies CAN signals
 * (steering, RPM, throttle, brake, gear) automatically.
 *
 * Usage: canbus_discover [device_prefix]
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
 * CAN Frame Buffer
 ******************************************************************************/

#define CLI_MAX_CAN_FRAMES 4096
static can_frame_t g_cli_can_frames[CLI_MAX_CAN_FRAMES];
static int g_cli_can_frame_count = 0;

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
    } else {
        _bleReady = NO;
        fprintf(stderr, "[WARN] Bluetooth not available (state: %ld)\n", (long)central.state);
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
    }
}

- (void)centralManager:(CBCentralManager *)central
  didConnectPeripheral:(CBPeripheral *)peripheral {
    _isConnecting = NO;
    _isConnected = YES;
    _connectedPeripheral = peripheral;
    peripheral.delegate = self;
    [peripheral discoverServices:@[[CBUUID UUIDWithString:kServiceUUID]]];
}

- (void)centralManager:(CBCentralManager *)central
didFailToConnectPeripheral:(CBPeripheral *)peripheral
                 error:(NSError *)error {
    fprintf(stderr, "[ERROR] Connection failed: %s\n", error.localizedDescription.UTF8String);
    _isConnecting = NO;
}

- (void)centralManager:(CBCentralManager *)central
didDisconnectPeripheral:(CBPeripheral *)peripheral
                 error:(NSError *)error {
    (void)error;
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
        fprintf(stderr, "[ERROR] Service discovery error: %s\n", error.localizedDescription.UTF8String);
        return;
    }

    for (CBService *service in peripheral.services) {
        [peripheral discoverCharacteristics:nil forService:service];
    }
}

- (void)peripheral:(CBPeripheral *)peripheral
didDiscoverCharacteristicsForService:(CBService *)service
             error:(NSError *)error {
    if (error) {
        fprintf(stderr, "[ERROR] Characteristic discovery error: %s\n", error.localizedDescription.UTF8String);
        return;
    }

    for (CBCharacteristic *characteristic in service.characteristics) {
        NSString *uuid = characteristic.UUID.UUIDString;

        if ([[uuid uppercaseString] containsString:@"FFF1"]) {
            _rxCharacteristic = characteristic;
        } else if ([[uuid uppercaseString] containsString:@"FFF2"]) {
            _txCharacteristic = characteristic;
            [peripheral setNotifyValue:YES forCharacteristic:characteristic];
        }
    }

    if (_rxCharacteristic && _txCharacteristic) {
        _serviceDiscoveryDone = YES;
    } else {
        fprintf(stderr, "[WARN] Missing required characteristics (RX=%s, TX=%s)\n",
                _rxCharacteristic ? "OK" : "missing",
                _txCharacteristic ? "OK" : "missing");
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

                    can_frame_t frame;
                    if (elm327_parse_can_frame(_canLineBuf, &frame)) {
                        if (g_cli_can_frame_count < CLI_MAX_CAN_FRAMES) {
                            g_cli_can_frames[g_cli_can_frame_count++] = frame;
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
    if (error) {
        fprintf(stderr, "[ERROR] Notification error: %s\n", error.localizedDescription.UTF8String);
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
        fprintf(stderr, "[ERROR] RFCOMM open failed (0x%08x)\n", error);
        return;
    }

    _isConnected = YES;
    _serviceDiscoveryDone = YES;
}

- (void)rfcommChannelData:(IOBluetoothRFCOMMChannel *)rfcommChannel
                     data:(void *)dataPointer
                   length:(size_t)dataLength {
    [self _handleReceivedBytes:(const char *)dataPointer length:(int)dataLength];
}

- (void)rfcommChannelClosed:(IOBluetoothRFCOMMChannel *)rfcommChannel {
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

    if (_bleReady) {
        [_central scanForPeripheralsWithServices:nil options:@{
            CBCentralManagerScanOptionAllowDuplicatesKey: @NO
        }];
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
        fprintf(stderr, "[ERROR] Invalid device index\n");
        return NO;
    }

    if (_isConnected) {
        fprintf(stderr, "[ERROR] Already connected. Disconnect first\n");
        return NO;
    }

    OBDTransportType transport = (OBDTransportType)[_discoveredTransports[index] intValue];
    int sourceIdx = [_discoveredSourceIndices[index] intValue];

    if (transport == OBD_TRANSPORT_BLE) {
        CBPeripheral *peripheral = _discoveredDevices[sourceIdx];
        _activeTransport = OBD_TRANSPORT_BLE;
        _isConnecting = YES;
        [_central connectPeripheral:peripheral options:nil];
    } else {
        IOBluetoothDevice *device = _classicDevices[sourceIdx];
        _activeTransport = OBD_TRANSPORT_CLASSIC;
        _isConnecting = YES;

        IOBluetoothRFCOMMChannel *channel = nil;
        IOReturn result = [device openRFCOMMChannelSync:&channel
                                          withChannelID:1
                                               delegate:self];
        _isConnecting = NO;

        if (result != kIOReturnSuccess || channel == nil) {
            fprintf(stderr, "[ERROR] RFCOMM connection failed (0x%08x)\n", result);
            return NO;
        }

        _rfcommChannel = channel;
        _connectedClassicDevice = device;
        _isConnected = YES;
        _serviceDiscoveryDone = YES;
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
        fprintf(stderr, "[ERROR] Not connected or service not ready\n");
        return NO;
    }

    if (_activeTransport == OBD_TRANSPORT_BLE && !_rxCharacteristic) {
        fprintf(stderr, "[ERROR] BLE characteristic not ready\n");
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
        fprintf(stderr, "[WARN] Command timeout: %s\n", cmd);
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
 * CLI Utilities
 ******************************************************************************/

/** Log to stderr */
static void cli_log(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void cli_log(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    fprintf(stderr, "[INFO] ");
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\n");
    va_end(ap);
}

/*******************************************************************************
 * OBD Provider Callbacks (stubs)
 ******************************************************************************/

/*******************************************************************************
 * BLE Connect
 ******************************************************************************/

/**
 * BLE scan + connect.
 * Scans, matches device by name prefix, connects, waits for service discovery.
 * If device_prefix is NULL, connects to first discovered device.
 * Returns matched device index, or exits with code 1 on failure.
 */
static int ble_connect(const char *device_prefix) {
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

    int matchIdx = -1;

    if (device_prefix) {
        /* Match device by name prefix (case-insensitive) */
        NSString *prefix = [[NSString stringWithUTF8String:device_prefix] uppercaseString];
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
    } else {
        /* No prefix — connect to first discovered device */
        matchIdx = 0;
        cli_log("Auto-selecting: %s (RSSI: %d)", g_ble.discoveredNames[0].UTF8String,
                g_ble.discoveredRSSIs[0].intValue);
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

/*******************************************************************************
 * PT-CAN Passive Connect
 ******************************************************************************/

/**
 * BLE connect + minimal AT init + silent mode.
 * No OBD protocol negotiation — adapter becomes a passive CAN sniffer.
 * If device_prefix is NULL, connects to first discovered device.
 * Returns matched device index, or exits with code 1 on failure.
 */
static int ptcan_connect(const char *device_prefix) {
    int matchIdx = ble_connect(device_prefix);
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

    cli_log("PT-CAN passive mode ready (silent, no ACK)");
    return matchIdx;
}

/*******************************************************************************
 * Signal Discovery
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

static int run_discover(const char *device_prefix) {
    int matchIdx = ptcan_connect(device_prefix);
    (void)matchIdx;

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
            fprintf(stderr, "  Skipped.\n");
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

/*******************************************************************************
 * Main
 ******************************************************************************/

int main(int argc, const char *argv[]) {

    @autoreleasepool {
        const char *device_prefix = NULL;

        if (argc >= 2) {
            if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0 ||
                strcmp(argv[1], "help") == 0) {
                fprintf(stderr, "Usage: canbus_discover [device_prefix]\n");
                fprintf(stderr, "\n");
                fprintf(stderr, "  PT-CAN signal auto-discovery tool.\n");
                fprintf(stderr, "  Connects via BLE/Classic BT, captures CAN traffic in 5 phases,\n");
                fprintf(stderr, "  and identifies steering/RPM/throttle/brake/gear signals.\n");
                fprintf(stderr, "\n");
                fprintf(stderr, "  device_prefix: BLE name prefix (e.g., vLinker, OBDLink)\n");
                fprintf(stderr, "                 If omitted, connects to first discovered device.\n");
                fprintf(stderr, "\n");
                fprintf(stderr, "  JSON output on stdout, logs on stderr.\n");
                fprintf(stderr, "  Use 2>/dev/null to get clean JSON.\n");
                return 0;
            }
            device_prefix = argv[1];
        }

        /* Initialize dependencies */
        kDeviceNamePrefixes = @[
            @"OBDLink", @"vLinker", @"VLINKER", @"VLinker",
            @"ELM327", @"OBD", @"iOBD"
        ];

        signal(SIGINT, signal_handler);

        g_ble = [[BLEManager alloc] init];

        int ret = run_discover(device_prefix);

        return ret;
    }
}
