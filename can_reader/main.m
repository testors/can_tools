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
#include <strings.h>
#include <stdarg.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <limits.h>
#include <ctype.h>

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

/** Max path buffer length for dump artifacts */
#define DISC_PATH_MAX 512

/** Transport type for discovered devices */
typedef enum { OBD_TRANSPORT_BLE, OBD_TRANSPORT_CLASSIC } OBDTransportType;

/*******************************************************************************
 * CLI Mode State (declared early for use in BLE delegate methods)
 ******************************************************************************/

static bool g_cli_mode = false;

#define CLI_MAX_CAN_FRAMES 20000
static can_frame_t g_cli_can_frames[CLI_MAX_CAN_FRAMES];
static int g_cli_can_frame_count = 0;
static const char *g_cli_monitor_out_path = NULL;

typedef struct {
    bool   skipped;
    bool   captured;
    int    planned_duration_s;
    double actual_duration_s;
    int    total_frames;
    int    id_count;
    char   dump_file[64];
} disc_phase_dump_meta_t;

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
                        frame.timestamp_ms =
                            (int64_t)([[NSDate date] timeIntervalSince1970] * 1000.0);
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

    /* Check already-paired Classic BT devices first (no inquiry needed) */
    NSArray *paired = [IOBluetoothDevice pairedDevices];
    for (IOBluetoothDevice *device in paired) {
        [self _addClassicDeviceIfMatched:device];
    }

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

static void disc_free_raw_phases(disc_raw_phase_t phases[DISC_NUM_PHASES]) {
    for (int i = 0; i < DISC_NUM_PHASES; i++) {
        disc_raw_phase_free(&phases[i]);
    }
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

static void json_print_string(FILE *f, const char *s);
static void cli_log(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static int cli_ble_connect(const char *device_prefix);
static int cli_auto_connect(const char *device_prefix);

/*******************************************************************************
 * Porsche 992 AiM Profile
 ******************************************************************************/

#define AIM992_MAX_CHANNELS      16
#define AIM992_MAX_RESPONSE_HEX  256
#define AIM992_MAX_PAYLOAD_BYTES 64

typedef enum {
    AIM992_QUERY_PID = 0,
    AIM992_QUERY_UDS
} aim992_query_kind_t;

typedef enum {
    AIM992_DECODE_STD = 0,
    AIM992_DECODE_PPS_AIM_PERCENT,
    AIM992_DECODE_MAP_MBAR,
    AIM992_DECODE_STEER_HEURISTIC,
    AIM992_DECODE_BRAKE_HEURISTIC
} aim992_decode_kind_t;

typedef struct {
    uint32_t tx_id;
    uint32_t rx_id;
} aim992_route_t;

typedef struct {
    char   long_name[48];
    char   short_name[16];
    char   unit[16];
    char   sens_name[48];
    int    idfis;
    int    fid;
    int    max_freq_ius;
    double lo;
    double hi;
    double conv1;
    bool   have_lo;
    bool   have_hi;
    bool   have_conv1;
} aim992_xml_channel_t;

typedef struct {
    const char            *long_name;
    const char            *short_name;
    aim992_query_kind_t    query_kind;
    aim992_decode_kind_t   decode_kind;
    uint8_t                pid;
    uint16_t               did;
    aim992_route_t         routes[4];
    int                    route_count;
    const char            *request_label;
    const char            *note;
} aim992_channel_spec_t;

typedef struct {
    bool     prepared;
    uint32_t tx_id;
    uint32_t rx_id;
    bool     flow_control_ready;
    uint32_t flow_control_tx_id;
} aim992_query_ctx_t;

typedef struct {
    const aim992_xml_channel_t  *xml;
    const aim992_channel_spec_t *spec;
    bool                         has_value;
    bool                         raw_available;
    bool                         heuristic;
    bool                         used_fallback;
    double                       value;
    uint32_t                     tx_id;
    uint32_t                     rx_id;
    char                         transport[24];
    char                         request[24];
    char                         unit[16];
    char                         raw_hex[AIM992_MAX_RESPONSE_HEX];
    char                         status[24];
    char                         note[160];
} aim992_result_t;

static const aim992_channel_spec_t s_aim992_specs[] = {
    { "RPM",            "RPM",  AIM992_QUERY_PID, AIM992_DECODE_STD,               0x0C, 0x0000,
      { { 0x7E0, 0x7E8 }, { 0x7DF, 0x7E8 } }, 2, "01 0C", "standard OBD-II PID; AiM companion uses functional 0x7DF" },
    { "THROTTLE_POS",   "THRT", AIM992_QUERY_PID, AIM992_DECODE_STD,               0x11, 0x0000,
      { { 0x7E0, 0x7E8 }, { 0x7DF, 0x7E8 } }, 2, "01 11", "standard OBD-II PID; AiM TPS channel, observed range about 12.55..85.10%" },
    { "ACCEL_POS",      "APS",  AIM992_QUERY_PID, AIM992_DECODE_PPS_AIM_PERCENT,   0x49, 0x0000,
      { { 0x7E0, 0x7E8 }, { 0x7DF, 0x7E8 } }, 2, "01 49", "AiM PPS channel; Mode 01 PID 49 raw with live-calibrated AiM scale 0x26->1.09%, 0xE5->104.89%" },
    { "COOLANT_TEMP",   "ECT",  AIM992_QUERY_PID, AIM992_DECODE_STD,               0x05, 0x0000,
      { { 0x7E0, 0x7E8 }, { 0x7DF, 0x7E8 } }, 2, "01 05", "standard OBD-II PID; AiM companion uses functional 0x7DF" },
    { "INTK_AIR_TEMP",  "IAT",  AIM992_QUERY_PID, AIM992_DECODE_STD,               0x0F, 0x0000,
      { { 0x7E0, 0x7E8 }, { 0x7DF, 0x7E8 } }, 2, "01 0F", "standard OBD-II PID; AiM companion uses functional 0x7DF" },
    { "MANIFOLD_PRESS", "MAP",  AIM992_QUERY_PID, AIM992_DECODE_MAP_MBAR,          0x0B, 0x0000,
      { { 0x7E0, 0x7E8 }, { 0x7DF, 0x7E8 } }, 2, "01 0B", "standard PID value converted from kPa to mbar to match XML unit" },
    { "FUEL_LEVEL",     "FLI",  AIM992_QUERY_PID, AIM992_DECODE_STD,               0x2F, 0x0000,
      { { 0x7E0, 0x7E8 }, { 0x7DF, 0x7E8 } }, 2, "01 2F", "standard PID inferred from XC1 mux 0x412F; AiM companion omits explicit 01 2F record" },
    { "STEER_ANGLE",    "STAN", AIM992_QUERY_UDS, AIM992_DECODE_STEER_HEURISTIC,   0x00, 0x5075,
      { { 0x70C, 0x776 }, { 0, 0 } }, 1, "22 50 75", "primary grouped chassis DID from passive AiM sniff; 22 51 01 kept as fallback until bytecode/live evidence disproves it" },
    { "BRAKE_PRESS",    "BRKP", AIM992_QUERY_UDS, AIM992_DECODE_BRAKE_HEURISTIC,   0x00, 0x2B21,
      { { 0x713, 0x77D }, { 0, 0 } }, 1, "22 2B 21", "confirmed by passive AiM sniff and GT3 XC1 companion BIN; scale from GT3 XC1 CANP" },
};

static const int s_aim992_spec_count =
    sizeof(s_aim992_specs) / sizeof(s_aim992_specs[0]);

static int aim992_hex_value(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    return -1;
}

static bool aim992_parse_hex_byte(const char *hex, uint8_t *out) {
    int hi = aim992_hex_value(hex[0]);
    int lo = aim992_hex_value(hex[1]);
    if (hi < 0 || lo < 0) return false;
    *out = (uint8_t)((hi << 4) | lo);
    return true;
}

static void aim992_copy_cstr(char *dst, size_t dst_len, const char *src) {
    if (!dst || dst_len == 0) return;
    snprintf(dst, dst_len, "%s", src ? src : "");
}

static void aim992_copy_nsstring(char *dst, size_t dst_len, NSString *src) {
    aim992_copy_cstr(dst, dst_len, src ? src.UTF8String : "");
}

static NSString *aim992_xpath_string(NSXMLNode *node, NSString *xpath) {
    NSError *err = nil;
    NSArray *matches = [node nodesForXPath:xpath error:&err];
    if (!matches || matches.count == 0) return nil;

    NSXMLNode *match = matches[0];
    NSString *value = match.stringValue;
    if (!value) return nil;
    return [value stringByTrimmingCharactersInSet:
            [NSCharacterSet whitespaceAndNewlineCharacterSet]];
}

static const aim992_channel_spec_t *aim992_find_spec_by_name(const char *name) {
    if (!name || !name[0]) return NULL;

    for (int i = 0; i < s_aim992_spec_count; i++) {
        const aim992_channel_spec_t *spec = &s_aim992_specs[i];
        if (strcasecmp(spec->long_name, name) == 0 ||
            strcasecmp(spec->short_name, name) == 0) {
            return spec;
        }
    }
    return NULL;
}

static int aim992_find_xml_channel_index(const aim992_xml_channel_t channels[],
                                         int channel_count,
                                         const char *name) {
    if (!name || !name[0]) return -1;

    for (int i = 0; i < channel_count; i++) {
        if (strcasecmp(channels[i].long_name, name) == 0 ||
            strcasecmp(channels[i].short_name, name) == 0) {
            return i;
        }
    }
    return -1;
}

static NSString *aim992_resolve_xml_path(void) {
    const char *env_path = getenv("CAN_TOOLS_992_XML");
    NSFileManager *fm = [NSFileManager defaultManager];

    if (env_path && env_path[0]) {
        NSString *candidate = [[NSString stringWithUTF8String:env_path]
                               stringByStandardizingPath];
        BOOL is_dir = NO;
        if ([fm fileExistsAtPath:candidate isDirectory:&is_dir] && !is_dir) {
            return candidate;
        }
    }

    NSMutableArray<NSString *> *bases = [NSMutableArray array];

    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        [bases addObject:[NSString stringWithUTF8String:cwd]];
    }

    NSString *exe_path = [NSBundle mainBundle].executablePath;
    if (exe_path.length > 0) {
        [bases addObject:[exe_path stringByDeletingLastPathComponent]];
    }

    NSArray<NSString *> *suffixes = @[
        @"ecus/PORSCHE_992_OBDII_65_20.xml",
        @"../ecus/PORSCHE_992_OBDII_65_20.xml",
        @"../../ecus/PORSCHE_992_OBDII_65_20.xml",
        @"992/PORSCHE_992_OBDII_65_20.xml",
        @"../992/PORSCHE_992_OBDII_65_20.xml",
        @"../../992/PORSCHE_992_OBDII_65_20.xml",
    ];

    for (NSString *base in bases) {
        for (NSString *suffix in suffixes) {
            NSString *candidate =
                [[base stringByAppendingPathComponent:suffix] stringByStandardizingPath];
            BOOL is_dir = NO;
            if ([fm fileExistsAtPath:candidate isDirectory:&is_dir] && !is_dir) {
                return candidate;
            }
        }
    }

    return nil;
}

static bool aim992_load_xml_channels(aim992_xml_channel_t channels[],
                                     int max_channels,
                                     int *channel_count_out,
                                     char *xml_path_out,
                                     size_t xml_path_out_len) {
    if (channel_count_out) *channel_count_out = 0;

    NSString *xml_path = aim992_resolve_xml_path();
    if (!xml_path) return false;

    NSError *read_err = nil;
    NSData *xml_data = [NSData dataWithContentsOfFile:xml_path options:0 error:&read_err];
    if (!xml_data) return false;

    NSError *parse_err = nil;
    NSXMLDocument *doc = [[NSXMLDocument alloc] initWithData:xml_data options:0 error:&parse_err];
    if (!doc) return false;

    NSError *xpath_err = nil;
    NSArray *nodes = [doc nodesForXPath:@"//e[@c='Canale']" error:&xpath_err];
    if (!nodes || nodes.count == 0) return false;

    int count = 0;
    for (NSXMLNode *node in nodes) {
        if (count >= max_channels) break;

        NSString *long_name = aim992_xpath_string(node, @"./p[@n='LongName']");
        if (long_name.length == 0) continue;

        aim992_xml_channel_t *ch = &channels[count];
        memset(ch, 0, sizeof(*ch));

        aim992_copy_nsstring(ch->long_name, sizeof(ch->long_name), long_name);
        aim992_copy_nsstring(ch->short_name, sizeof(ch->short_name),
                             aim992_xpath_string(node, @"./p[@n='ShortName']"));
        aim992_copy_nsstring(ch->unit, sizeof(ch->unit),
                             aim992_xpath_string(node, @"./e[@c='SENS']/p[@n='UIB']"));
        aim992_copy_nsstring(ch->sens_name, sizeof(ch->sens_name),
                             aim992_xpath_string(node, @"./e[@c='SENS']/@i"));

        NSString *idfis_str = aim992_xpath_string(node, @"./p[@n='IDFis']");
        NSString *fid_str = aim992_xpath_string(node, @"./p[@n='FID']");
        NSString *max_freq_str = aim992_xpath_string(node, @"./p[@n='MaxFreqIus']");
        NSString *lo_str = aim992_xpath_string(node, @"./e[@c='SENS']/p[@n='Lo']");
        NSString *hi_str = aim992_xpath_string(node, @"./e[@c='SENS']/p[@n='Hi']");
        NSString *conv1_str = aim992_xpath_string(node,
                                                  @"./e[@c='SENS']/e[@c='ConvFunc']/p[@n='1']");

        ch->idfis = idfis_str.length > 0 ? idfis_str.intValue : -1;
        ch->fid = fid_str.length > 0 ? fid_str.intValue : -1;
        ch->max_freq_ius = max_freq_str.length > 0 ? max_freq_str.intValue : 0;
        if (lo_str.length > 0) {
            ch->lo = lo_str.doubleValue;
            ch->have_lo = true;
        }
        if (hi_str.length > 0) {
            ch->hi = hi_str.doubleValue;
            ch->have_hi = true;
        }
        if (conv1_str.length > 0) {
            ch->conv1 = conv1_str.doubleValue;
            ch->have_conv1 = true;
        }

        count++;
    }

    if (channel_count_out) *channel_count_out = count;
    if (xml_path_out && xml_path_out_len > 0) {
        aim992_copy_nsstring(xml_path_out, xml_path_out_len, xml_path);
    }
    return count > 0;
}

static int aim992_select_channels(const aim992_xml_channel_t channels[],
                                  int channel_count,
                                  int name_argc,
                                  const char *name_argv[],
                                  int selected_indices[],
                                  int max_selected) {
    int selected_count = 0;

    if (name_argc == 0) {
        for (int i = 0; i < channel_count && selected_count < max_selected; i++) {
            selected_indices[selected_count++] = i;
        }
        return selected_count;
    }

    for (int i = 0; i < name_argc && selected_count < max_selected; i++) {
        int idx = aim992_find_xml_channel_index(channels, channel_count, name_argv[i]);
        if (idx < 0) return -1;

        bool duplicate = false;
        for (int j = 0; j < selected_count; j++) {
            if (selected_indices[j] == idx) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            selected_indices[selected_count++] = idx;
        }
    }

    return selected_count;
}

static bool aim992_result_value_in_range(const aim992_xml_channel_t *xml, double value) {
    if (!xml) return true;
    if (xml->have_lo && value < xml->lo - 1e-6) return false;
    if (xml->have_hi && value > xml->hi + 1e-6) return false;
    return true;
}

static void aim992_result_init(aim992_result_t *result,
                               const aim992_xml_channel_t *xml,
                               const aim992_channel_spec_t *spec) {
    memset(result, 0, sizeof(*result));
    result->xml = xml;
    result->spec = spec;
    if (xml) {
        aim992_copy_cstr(result->unit, sizeof(result->unit), xml->unit);
    }
    if (spec) {
        aim992_copy_cstr(result->request, sizeof(result->request), spec->request_label);
        aim992_copy_cstr(result->note, sizeof(result->note), spec->note);
        result->tx_id = spec->route_count > 0 ? spec->routes[0].tx_id : 0;
        result->rx_id = spec->route_count > 0 ? spec->routes[0].rx_id : 0;
    }
    aim992_copy_cstr(result->status, sizeof(result->status), "pending");
}

static bool aim992_send_command_ok(const char *cmd, char *resp, size_t resp_len) {
    if (!cmd || !resp || resp_len == 0) return false;
    if (![g_ble sendCommand:cmd response:resp maxLen:(int)resp_len]) return false;
    return !elm327_is_error_response(resp);
}

static bool aim992_prepare_query_context(aim992_query_ctx_t *ctx) {
    if (!ctx) return false;
    if (ctx->prepared) return true;

    char resp[256];
    if (!aim992_send_command_ok("ATSP6", resp, sizeof(resp))) return false;

    if (![g_ble sendCommand:"ATCSM0" response:resp maxLen:sizeof(resp)]) {
        /* best effort */
    }
    [g_ble sendCommand:"ATAT2" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATST 0A" response:resp maxLen:sizeof(resp)];
    if (g_stn_detected) {
        [g_ble sendCommand:"STPTO 0A" response:resp maxLen:sizeof(resp)];
    }
    [g_ble sendCommand:"ATS0" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATH1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATCAF1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAL" response:resp maxLen:sizeof(resp)];

    ctx->prepared = true;
    ctx->tx_id = UINT32_MAX;
    ctx->rx_id = UINT32_MAX;
    ctx->flow_control_ready = false;
    ctx->flow_control_tx_id = UINT32_MAX;
    return true;
}

static bool aim992_set_route(aim992_query_ctx_t *ctx, aim992_route_t route) {
    if (!ctx) return false;
    if (!aim992_prepare_query_context(ctx)) return false;

    if (ctx->tx_id == route.tx_id && ctx->rx_id == route.rx_id) {
        return true;
    }

    char cmd[32];
    char resp[256];

    snprintf(cmd, sizeof(cmd), "ATSH %03X", route.tx_id);
    if (!aim992_send_command_ok(cmd, resp, sizeof(resp))) return false;

    snprintf(cmd, sizeof(cmd), "ATCRA %03X", route.rx_id);
    if (!aim992_send_command_ok(cmd, resp, sizeof(resp))) return false;

    ctx->tx_id = route.tx_id;
    ctx->rx_id = route.rx_id;
    return true;
}

static void aim992_configure_flow_control(aim992_route_t route) {
    char cmd[32];
    char resp[256];

    /* AiM uses 30 00 01 on the same tester CAN ID for 992 multi-frame DIDs. */
    [g_ble sendCommand:"ATCFC1" response:resp maxLen:sizeof(resp)];
    snprintf(cmd, sizeof(cmd), "ATFCSH%03X", route.tx_id);
    [g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATFCSD300001" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATFCSM1" response:resp maxLen:sizeof(resp)];
}

static bool aim992_ensure_flow_control(aim992_query_ctx_t *ctx, aim992_route_t route) {
    if (!ctx) return false;

    char cmd[32];
    char resp[256];

    if (!ctx->flow_control_ready) {
        if (!aim992_send_command_ok("ATCFC1", resp, sizeof(resp))) return false;
        snprintf(cmd, sizeof(cmd), "ATFCSH%03X", route.tx_id);
        if (!aim992_send_command_ok(cmd, resp, sizeof(resp))) return false;
        if (!aim992_send_command_ok("ATFCSD300001", resp, sizeof(resp))) return false;
        if (!aim992_send_command_ok("ATFCSM1", resp, sizeof(resp))) return false;

        ctx->flow_control_ready = true;
        ctx->flow_control_tx_id = route.tx_id;
        return true;
    }

    if (ctx->flow_control_tx_id == route.tx_id) {
        return true;
    }

    snprintf(cmd, sizeof(cmd), "ATFCSH%03X", route.tx_id);
    if (!aim992_send_command_ok(cmd, resp, sizeof(resp))) return false;

    ctx->flow_control_tx_id = route.tx_id;
    return true;
}

static bool aim992_extract_after_marker(const char *response,
                                        const char *marker_hex,
                                        uint8_t *payload_out,
                                        int max_payload,
                                        int *payload_len_out,
                                        char *raw_hex_out,
                                        size_t raw_hex_out_len) {
    if (payload_len_out) *payload_len_out = 0;

    char hex[AIM992_MAX_RESPONSE_HEX];
    int hex_len = elm327_extract_hex(response, hex, sizeof(hex));
    if (raw_hex_out && raw_hex_out_len > 0) {
        aim992_copy_cstr(raw_hex_out, raw_hex_out_len, hex);
    }
    if (hex_len <= 0) return false;

    const char *marker = strstr(hex, marker_hex);
    if (!marker) return false;

    int offset = (int)(marker - hex) + (int)strlen(marker_hex);
    int payload_len = 0;
    while (offset + 1 < hex_len && payload_len < max_payload) {
        if (!aim992_parse_hex_byte(hex + offset, &payload_out[payload_len])) return false;
        payload_len++;
        offset += 2;
    }

    if (payload_len_out) *payload_len_out = payload_len;
    return true;
}

static const char *aim992_uds_nrc_name(uint8_t nrc) {
    switch (nrc) {
        case 0x10: return "generalReject";
        case 0x11: return "serviceNotSupported";
        case 0x12: return "subFunctionNotSupported";
        case 0x13: return "incorrectMessageLengthOrInvalidFormat";
        case 0x22: return "conditionsNotCorrect";
        case 0x31: return "requestOutOfRange";
        case 0x33: return "securityAccessDenied";
        case 0x78: return "responsePending";
        default:   return "unknown";
    }
}

static bool aim992_find_uds_negative_response(const char *hex,
                                              uint8_t service_id,
                                              uint8_t *nrc_out) {
    if (!hex || !hex[0]) return false;

    char marker[8];
    snprintf(marker, sizeof(marker), "7F%02X", service_id);

    const char *p = strstr(hex, marker);
    if (!p) return false;

    int offset = (int)(p - hex) + 4;
    if (offset + 1 >= (int)strlen(hex)) return false;

    uint8_t nrc = 0;
    if (!aim992_parse_hex_byte(hex + offset, &nrc)) return false;
    if (nrc_out) *nrc_out = nrc;
    return true;
}

static bool aim992_find_uds_positive_did_response(const char *hex, uint16_t did) {
    if (!hex || !hex[0]) return false;

    char marker[12];
    snprintf(marker, sizeof(marker), "62%02X%02X", did >> 8, did & 0xFF);
    return strstr(hex, marker) != NULL;
}

static bool aim992_find_uds_positive_session_response(const char *hex, uint8_t session) {
    if (!hex || !hex[0]) return false;

    char marker[8];
    snprintf(marker, sizeof(marker), "50%02X", session);
    return strstr(hex, marker) != NULL;
}

static int aim992_extract_line_hex(const char *line, char *hex_out, size_t hex_out_len) {
    size_t j = 0;
    if (!line || !hex_out || hex_out_len == 0) return 0;

    for (size_t i = 0; line[i] != '\0' && j + 1 < hex_out_len; i++) {
        if (aim992_hex_value(line[i]) >= 0) {
            hex_out[j++] = (char)toupper((unsigned char)line[i]);
        }
    }
    hex_out[j] = '\0';
    return (int)j;
}

static bool aim992_parse_isotp_payload(const char *response,
                                       uint8_t *payload_out,
                                       int max_payload,
                                       int *payload_len_out,
                                       uint32_t *resp_id_out) {
    if (payload_len_out) *payload_len_out = 0;
    if (!response || !payload_out || max_payload <= 0) return false;

    char buf[1024];
    aim992_copy_cstr(buf, sizeof(buf), response);

    int payload_len = 0;
    int total_len = -1;
    bool started = false;
    uint32_t resp_id = UINT32_MAX;

    for (char *line = strtok(buf, "\r\n"); line; line = strtok(NULL, "\r\n")) {
        char hex[128];
        int hex_len = aim992_extract_line_hex(line, hex, sizeof(hex));
        if (hex_len < 5) continue;

        unsigned int id_val = 0;
        if (sscanf(hex, "%3x", &id_val) != 1) continue;

        const char *data_hex = hex + 3;
        int data_hex_len = hex_len - 3;
        if (data_hex_len < 2) continue;

        uint8_t data[32];
        int data_len = 0;
        for (int i = 0; i + 1 < data_hex_len && data_len < (int)sizeof(data); i += 2) {
            if (!aim992_parse_hex_byte(data_hex + i, &data[data_len])) break;
            data_len++;
        }
        if (data_len <= 0) continue;

        uint8_t pci = data[0];
        uint8_t frame_type = (uint8_t)(pci >> 4);
        if (frame_type == 0x0) {
            int len = pci & 0x0F;
            if (len > data_len - 1) len = data_len - 1;
            if (len <= 0) continue;
            if (len > max_payload) len = max_payload;
            memcpy(payload_out, data + 1, (size_t)len);
            payload_len = len;
            total_len = len;
            resp_id = id_val;
            started = true;
            break;
        }

        if (frame_type == 0x1) {
            total_len = ((pci & 0x0F) << 8) | data[1];
            int copy_len = data_len - 2;
            if (copy_len < 0) copy_len = 0;
            if (copy_len > max_payload) copy_len = max_payload;
            memcpy(payload_out, data + 2, (size_t)copy_len);
            payload_len = copy_len;
            resp_id = id_val;
            started = true;
            if (payload_len >= total_len) break;
            continue;
        }

        if (frame_type == 0x2 && started) {
            int copy_len = data_len - 1;
            if (copy_len <= 0) continue;
            if (payload_len + copy_len > max_payload) {
                copy_len = max_payload - payload_len;
            }
            if (copy_len <= 0) break;
            memcpy(payload_out + payload_len, data + 1, (size_t)copy_len);
            payload_len += copy_len;
            if (total_len >= 0 && payload_len >= total_len) break;
        }
    }

    if (!started || payload_len <= 0) return false;
    if (total_len >= 0 && payload_len > total_len) payload_len = total_len;
    if (payload_len_out) *payload_len_out = payload_len;
    if (resp_id_out) *resp_id_out = resp_id;
    return true;
}

static void aim992_format_payload_hex(const uint8_t *payload,
                                      int payload_len,
                                      char *hex_out,
                                      size_t hex_out_len) {
    if (!hex_out || hex_out_len == 0) return;
    hex_out[0] = '\0';
    if (!payload || payload_len <= 0) return;

    size_t pos = 0;
    for (int i = 0; i < payload_len && pos + 3 < hex_out_len; i++) {
        pos += (size_t)snprintf(hex_out + pos, hex_out_len - pos, "%02X", payload[i]);
    }
}

static bool aim992_payload_ascii(const uint8_t *payload,
                                 int payload_len,
                                 int offset,
                                 char *text_out,
                                 size_t text_out_len) {
    if (!text_out || text_out_len == 0) return false;
    text_out[0] = '\0';
    if (!payload || payload_len <= offset) return false;

    size_t pos = 0;
    for (int i = offset; i < payload_len && pos + 1 < text_out_len; i++) {
        uint8_t b = payload[i];
        if (b < 0x20 || b > 0x7E) return false;
        text_out[pos++] = (char)b;
    }
    if (pos == 0) return false;
    text_out[pos] = '\0';
    return true;
}

static void aim992_fill_pid_result(aim992_result_t *result,
                                   const uint8_t *data,
                                   int data_len,
                                   const char *raw_hex);

static bool aim992_query_pid(aim992_query_ctx_t *ctx,
                             uint8_t pid,
                             aim992_route_t route,
                             uint8_t *data_out,
                             int *data_len_out,
                             char *raw_hex_out,
                             size_t raw_hex_out_len) {
    if (!ctx || !data_out || !data_len_out) return false;
    if (!aim992_set_route(ctx, route)) return false;

    char cmd[16];
    char resp[512];
    char marker[8];

    elm327_build_pid_query(pid, cmd, sizeof(cmd));
    if (!aim992_send_command_ok(cmd, resp, sizeof(resp))) return false;

    snprintf(marker, sizeof(marker), "41%02X", pid);

    uint8_t payload[ELM327_MAX_DATA_BYTES];
    int payload_len = 0;
    if (!aim992_extract_after_marker(resp, marker, payload, sizeof(payload),
                                     &payload_len, raw_hex_out, raw_hex_out_len)) {
        return false;
    }

    const elm327_pid_info_t *info = elm327_get_pid_info(pid);
    int expected_len = info ? info->data_bytes : payload_len;
    if (expected_len > payload_len) return false;

    memcpy(data_out, payload, (size_t)expected_len);
    *data_len_out = expected_len;
    return true;
}

static int aim992_mode01_response_bytes(uint8_t pid) {
    const elm327_pid_info_t *info = elm327_get_pid_info(pid);
    return 1 + (info ? info->data_bytes : 1);
}

static bool aim992_find_mode01_batch_data(const uint8_t *payload,
                                          int payload_len,
                                          uint8_t pid,
                                          uint8_t *data_out,
                                          int *data_len_out) {
    if (!payload || payload_len < 2 || !data_out || !data_len_out) return false;
    if (payload[0] != 0x41) return false;

    int offset = 1;
    while (offset < payload_len) {
        uint8_t candidate_pid = payload[offset++];
        const elm327_pid_info_t *info = elm327_get_pid_info(candidate_pid);
        int data_len = info ? info->data_bytes : 1;
        if (data_len <= 0 || offset + data_len > payload_len) return false;

        if (candidate_pid == pid) {
            memcpy(data_out, payload + offset, (size_t)data_len);
            *data_len_out = data_len;
            return true;
        }

        offset += data_len;
    }

    return false;
}

static bool aim992_query_pid_batch(aim992_query_ctx_t *ctx,
                                   const aim992_channel_spec_t *specs[],
                                   const int result_indices[],
                                   int pid_count,
                                   aim992_route_t route,
                                   aim992_result_t results[]) {
    if (!ctx || !specs || !result_indices || !results ||
        pid_count <= 1 || pid_count > STN2255_MAX_BATCH_PIDS) {
        return false;
    }
    if (!aim992_set_route(ctx, route)) return false;

    uint8_t pids[STN2255_MAX_BATCH_PIDS];
    int expected_response_payload_len = 1;
    for (int i = 0; i < pid_count; i++) {
        pids[i] = specs[i]->pid;
        expected_response_payload_len += aim992_mode01_response_bytes(specs[i]->pid);
    }

    if (expected_response_payload_len > 7 && route.tx_id != 0x7DF) {
        if (!aim992_ensure_flow_control(ctx, route)) return false;
    }

    char cmd[STN2255_CMD_MAX_LEN];
    if (stn2255_build_batch_query(pids, pid_count, cmd, sizeof(cmd)) != 0) {
        return false;
    }

    char resp[1024];
    if (!aim992_send_command_ok(cmd, resp, sizeof(resp))) return false;

    char raw_hex[AIM992_MAX_RESPONSE_HEX] = "";
    elm327_extract_hex(resp, raw_hex, sizeof(raw_hex));

    uint8_t isotp_payload[AIM992_MAX_PAYLOAD_BYTES];
    int isotp_payload_len = 0;
    uint32_t resp_id = UINT32_MAX;
    if (aim992_parse_isotp_payload(resp, isotp_payload, sizeof(isotp_payload),
                                   &isotp_payload_len, &resp_id)) {
        bool any = false;
        for (int i = 0; i < pid_count; i++) {
            uint8_t data[ELM327_MAX_DATA_BYTES];
            int data_len = 0;
            if (!aim992_find_mode01_batch_data(isotp_payload, isotp_payload_len,
                                               specs[i]->pid, data, &data_len)) {
                continue;
            }

            aim992_result_t *res = &results[result_indices[i]];
            if (res->has_value) continue;

            res->tx_id = route.tx_id;
            res->rx_id = route.rx_id;
            aim992_fill_pid_result(res, data, data_len, raw_hex);
            aim992_copy_cstr(res->transport, sizeof(res->transport), "mode01_batch");
            any = true;
        }
        if (any) return true;
    }

    stn2255_batch_result_t batch;
    if (stn2255_parse_batch_response(resp, &batch) <= 0) return false;

    bool any = false;
    for (int i = 0; i < pid_count; i++) {
        aim992_result_t *res = &results[result_indices[i]];
        if (res->has_value) continue;

        for (int j = 0; j < batch.count; j++) {
            if (!batch.results[j].valid || batch.results[j].pid != specs[i]->pid) continue;

            res->tx_id = route.tx_id;
            res->rx_id = route.rx_id;
            aim992_fill_pid_result(res, batch.results[j].data,
                                   batch.results[j].data_len, raw_hex);
            aim992_copy_cstr(res->transport, sizeof(res->transport), "mode01_batch");
            any = true;
            break;
        }
    }

    return any;
}

static bool aim992_query_did(aim992_query_ctx_t *ctx,
                             uint16_t did,
                             const aim992_route_t routes[],
                             int route_count,
                             uint8_t *payload_out,
                             int *payload_len_out,
                             char *raw_hex_out,
                             size_t raw_hex_out_len,
                             aim992_route_t *matched_route_out,
                             char *failure_note_out,
                             size_t failure_note_out_len) {
    if (!ctx || !routes || route_count <= 0 || !payload_out || !payload_len_out) return false;
    if (failure_note_out && failure_note_out_len > 0) failure_note_out[0] = '\0';

    char cmd[16];
    char resp[512];
    char marker[12];

    snprintf(cmd, sizeof(cmd), "22%02X%02X", did >> 8, did & 0xFF);
    snprintf(marker, sizeof(marker), "62%02X%02X", did >> 8, did & 0xFF);

	for (int i = 0; i < route_count; i++) {
	    if (!aim992_set_route(ctx, routes[i])) continue;
	    if (!aim992_ensure_flow_control(ctx, routes[i])) continue;
	    if (!aim992_send_command_ok(cmd, resp, sizeof(resp))) continue;

	    uint8_t isotp_payload[AIM992_MAX_PAYLOAD_BYTES];
	    int isotp_payload_len = 0;
	    uint32_t resp_id = UINT32_MAX;
	    if (aim992_parse_isotp_payload(resp, isotp_payload, sizeof(isotp_payload),
	                                   &isotp_payload_len, &resp_id)) {
	        if (isotp_payload_len >= 3 &&
	            isotp_payload[0] == 0x62 &&
	            isotp_payload[1] == (uint8_t)(did >> 8) &&
	            isotp_payload[2] == (uint8_t)(did & 0xFF)) {
	            int payload_len = isotp_payload_len - 3;
	            if (payload_len > AIM992_MAX_PAYLOAD_BYTES) payload_len = AIM992_MAX_PAYLOAD_BYTES;
	            memcpy(payload_out, isotp_payload + 3, (size_t)payload_len);
	            *payload_len_out = payload_len;
	            if (raw_hex_out && raw_hex_out_len > 0) {
	                aim992_format_payload_hex(isotp_payload, isotp_payload_len,
	                                          raw_hex_out, raw_hex_out_len);
	            }
	            if (matched_route_out) *matched_route_out = routes[i];
	            return true;
	        }

	        if (isotp_payload_len >= 3 &&
	            isotp_payload[0] == 0x7F &&
	            isotp_payload[1] == 0x22 &&
	            failure_note_out && failure_note_out_len > 0) {
	            if (matched_route_out) *matched_route_out = routes[i];
	            snprintf(failure_note_out, failure_note_out_len,
	                     "No positive response to 22 %02X %02X; last negative response was %03X->%03X NRC 0x%02X (%s)",
	                     did >> 8, did & 0xFF,
	                     routes[i].tx_id, routes[i].rx_id,
	                     isotp_payload[2], aim992_uds_nrc_name(isotp_payload[2]));
	        }
	        continue;
	    }

	    int payload_len = 0;
	    if (!aim992_extract_after_marker(resp, marker, payload_out, AIM992_MAX_PAYLOAD_BYTES,
                                         &payload_len, raw_hex_out, raw_hex_out_len)) {
            char hex[AIM992_MAX_RESPONSE_HEX] = "";
            uint8_t nrc = 0;
            elm327_extract_hex(resp, hex, sizeof(hex));
            if (aim992_find_uds_negative_response(hex, 0x22, &nrc) &&
                failure_note_out && failure_note_out_len > 0) {
                if (matched_route_out) *matched_route_out = routes[i];
                snprintf(failure_note_out, failure_note_out_len,
                         "No positive response to 22 %02X %02X; last negative response was %03X->%03X NRC 0x%02X (%s)",
                         did >> 8, did & 0xFF,
                         routes[i].tx_id, routes[i].rx_id,
                         nrc, aim992_uds_nrc_name(nrc));
            }
            continue;
        }

        *payload_len_out = payload_len;
        if (matched_route_out) *matched_route_out = routes[i];
        return true;
    }

    return false;
}

static int16_t aim992_read_s16_be(const uint8_t *p) {
    return (int16_t)((p[0] << 8) | p[1]);
}

static int16_t aim992_read_s16_le(const uint8_t *p) {
    return (int16_t)((p[1] << 8) | p[0]);
}

static uint16_t aim992_read_u16_be(const uint8_t *p) {
    return (uint16_t)((p[0] << 8) | p[1]);
}

static bool aim992_decode_steer_guess(const aim992_xml_channel_t *xml,
                                      const uint8_t *payload,
                                      int payload_len,
                                      double *value_out,
                                      char *note_out,
                                      size_t note_out_len) {
    double scale = (xml && xml->have_conv1) ? xml->conv1 : 0.1;
    const struct { int offset; bool little; } candidates[] = {
        { 0, false }, { 2, false }, { 0, true }, { 2, true },
    };

    for (int i = 0; i < 4; i++) {
        int offset = candidates[i].offset;
        if (offset + 1 >= payload_len) continue;

        int16_t raw = candidates[i].little
            ? aim992_read_s16_le(payload + offset)
            : aim992_read_s16_be(payload + offset);
        double value = raw * scale;
        if (!aim992_result_value_in_range(xml, value)) continue;

        if (value_out) *value_out = value;
        if (note_out && note_out_len > 0) {
            snprintf(note_out, note_out_len,
                     "heuristic DID 0x5101 decode: %s-endian signed16 payload[%d..%d] x %.3g",
                     candidates[i].little ? "little" : "big",
                     offset, offset + 1, scale);
        }
        return true;
    }

    return false;
}

static bool aim992_decode_steer_5075_guess(const aim992_xml_channel_t *xml,
                                           const uint8_t *payload,
                                           int payload_len,
                                           double *value_out,
                                           char *note_out,
                                           size_t note_out_len) {
    if (!payload || payload_len <= 9) return false;

    const int lane_a = payload[7];
    const int lane_b = payload[9];
    const bool complement_pair = (lane_a + lane_b) == 200;

    const double candidate_a = ((double)lane_a - 128.0) / 10.0;
    const double candidate_b = ((double)lane_b - 100.0) / 10.0;

    if (aim992_result_value_in_range(xml, candidate_a)) {
        if (value_out) *value_out = candidate_a;
        if (note_out && note_out_len > 0) {
            snprintf(note_out, note_out_len,
                     "heuristic DID 0x5075 decode: payload[7] -> (byte-128)/10 = %.3f deg%s; mirror lane payload[9] -> (byte-100)/10 = %.3f deg",
                     candidate_a,
                     complement_pair ? ", payload[7]+payload[9]=200" : "",
                     candidate_b);
        }
        return true;
    }

    if (aim992_result_value_in_range(xml, candidate_b)) {
        if (value_out) *value_out = candidate_b;
        if (note_out && note_out_len > 0) {
            snprintf(note_out, note_out_len,
                     "heuristic DID 0x5075 decode: payload[9] -> (byte-100)/10 = %.3f deg%s; alternate lane payload[7] -> (byte-128)/10 = %.3f deg",
                     candidate_b,
                     complement_pair ? ", payload[7]+payload[9]=200" : "",
                     candidate_a);
        }
        return true;
    }

    return false;
}

static bool aim992_decode_brake_2b21_guess(const aim992_xml_channel_t *xml,
                                           const uint8_t *payload,
                                           int payload_len,
                                           double *value_out,
                                           char *note_out,
                                           size_t note_out_len) {
    if (!payload || payload_len < 2) return false;

    const uint16_t raw = aim992_read_u16_be(payload);
    const double value_unclamped = ((double)raw * 0.016) - 44.451;
    const double value = value_unclamped > 0.0 ? value_unclamped : 0.0;

    if (!aim992_result_value_in_range(xml, value)) return false;

    if (value_out) *value_out = value;
    if (note_out && note_out_len > 0) {
        snprintf(note_out, note_out_len,
                 "GT3 XC1 DID 0x2B21 decode: big-endian raw payload[0..1]=0x%04X, raw*0.016-44.451 = %.3f bar",
                 raw, value);
    }
    return true;
}

static void aim992_fill_pid_result(aim992_result_t *result,
                                   const uint8_t *data,
                                   int data_len,
                                   const char *raw_hex) {
    if (!result || !result->spec || !result->xml) return;

	    double value = elm327_convert_pid_value(result->spec->pid, data, data_len);
	    switch (result->spec->decode_kind) {
	        case AIM992_DECODE_PPS_AIM_PERCENT:
	            if (data_len >= 1) {
	                value = 1.09 + (((double)data[0] - 38.0) * (103.80 / 191.0));
	                if (value < 0.0) value = 0.0;
	            }
	            break;
	        case AIM992_DECODE_MAP_MBAR:
	            value *= 10.0;
	            aim992_copy_cstr(result->unit, sizeof(result->unit), "mbar");
            break;
        case AIM992_DECODE_STD:
        default:
            break;
    }

    result->has_value = true;
    result->raw_available = (raw_hex && raw_hex[0]);
    if (raw_hex) {
        aim992_copy_cstr(result->raw_hex, sizeof(result->raw_hex), raw_hex);
    }
    aim992_copy_cstr(result->transport, sizeof(result->transport), "mode01");
    aim992_copy_cstr(result->status, sizeof(result->status), "ok");
    result->value = value;
}

static void aim992_print_list_item(FILE *f,
                                   const aim992_xml_channel_t *xml,
                                   const aim992_channel_spec_t *spec) {
    char tx_id[8] = "";

    if (spec && spec->route_count > 0) {
        snprintf(tx_id, sizeof(tx_id), "%03X", spec->routes[0].tx_id);
    }

    fprintf(f, "  {\"long_name\":");
    json_print_string(f, xml->long_name);
    fprintf(f, ",\"short_name\":");
    json_print_string(f, xml->short_name);
    fprintf(f, ",\"idfis\":%d,\"fid\":%d", xml->idfis, xml->fid);
    fprintf(f, ",\"unit\":");
    json_print_string(f, xml->unit);
    fprintf(f, ",\"sens\":");
    json_print_string(f, xml->sens_name);
    if (xml->have_lo) fprintf(f, ",\"lo\":%.6g", xml->lo);
    if (xml->have_hi) fprintf(f, ",\"hi\":%.6g", xml->hi);
    if (xml->have_conv1) fprintf(f, ",\"conv1\":%.6g", xml->conv1);
    if (xml->max_freq_ius > 0) {
        fprintf(f, ",\"max_freq_hz\":%.3f", 1000000.0 / (double)xml->max_freq_ius);
    }
    if (spec) {
        fprintf(f, ",\"transport\":");
        json_print_string(f, spec->query_kind == AIM992_QUERY_PID ? "mode01" : "uds");
        fprintf(f, ",\"request\":");
        json_print_string(f, spec->request_label);
        fprintf(f, ",\"tx_id\":");
        json_print_string(f, tx_id);
        fprintf(f, ",\"rx_ids\":[");
        bool first_rx = true;
        for (int i = 0; i < spec->route_count; i++) {
            char rx_id[8];
            bool duplicate = false;
            for (int j = 0; j < i; j++) {
                if (spec->routes[j].rx_id == spec->routes[i].rx_id) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) continue;
            snprintf(rx_id, sizeof(rx_id), "%03X", spec->routes[i].rx_id);
            if (!first_rx) fprintf(f, ",");
            json_print_string(f, rx_id);
            first_rx = false;
        }
        fprintf(f, "]");
        fprintf(f, ",\"routes\":[");
        for (int i = 0; i < spec->route_count; i++) {
            char route_tx[8];
            char route_rx[8];
            snprintf(route_tx, sizeof(route_tx), "%03X", spec->routes[i].tx_id);
            snprintf(route_rx, sizeof(route_rx), "%03X", spec->routes[i].rx_id);
            if (i > 0) fprintf(f, ",");
            fprintf(f, "{\"tx_id\":");
            json_print_string(f, route_tx);
            fprintf(f, ",\"rx_id\":");
            json_print_string(f, route_rx);
            fprintf(f, "}");
        }
        fprintf(f, "]");
        fprintf(f, ",\"decode\":");
        switch (spec->decode_kind) {
            case AIM992_DECODE_STEER_HEURISTIC:
            case AIM992_DECODE_BRAKE_HEURISTIC:
                json_print_string(f, "heuristic");
                break;
            default:
                json_print_string(f, "documented");
                break;
        }
        fprintf(f, ",\"note\":");
        json_print_string(f, spec->note);
    } else {
        fprintf(f, ",\"transport\":\"unmapped\"");
    }
    fprintf(f, "}");
}

static void aim992_print_result_item(FILE *f, const aim992_result_t *result) {
    char tx_id[8] = "";
    char rx_id[8] = "";

    if (result->tx_id <= 0x7FF) snprintf(tx_id, sizeof(tx_id), "%03X", result->tx_id);
    if (result->rx_id <= 0x7FF) snprintf(rx_id, sizeof(rx_id), "%03X", result->rx_id);

    fprintf(f, "  {\"channel\":");
    json_print_string(f, result->xml->long_name);
    fprintf(f, ",\"short_name\":");
    json_print_string(f, result->xml->short_name);
    fprintf(f, ",\"status\":");
    json_print_string(f, result->status);
    fprintf(f, ",\"value\":");
    if (result->has_value) fprintf(f, "%.6f", result->value);
    else fprintf(f, "null");
    fprintf(f, ",\"unit\":");
    json_print_string(f, result->unit);
    fprintf(f, ",\"transport\":");
    json_print_string(f, result->transport);
    fprintf(f, ",\"request\":");
    json_print_string(f, result->request);
    fprintf(f, ",\"tx_id\":");
    json_print_string(f, tx_id);
    fprintf(f, ",\"rx_id\":");
    json_print_string(f, rx_id);
    fprintf(f, ",\"raw\":");
    if (result->raw_available) json_print_string(f, result->raw_hex);
    else fprintf(f, "null");
    fprintf(f, ",\"heuristic\":%s", result->heuristic ? "true" : "false");
    fprintf(f, ",\"fallback\":%s", result->used_fallback ? "true" : "false");
    fprintf(f, ",\"note\":");
    json_print_string(f, result->note);
    fprintf(f, "}");
}

static bool aim992_collect_snapshot(aim992_query_ctx_t *ctx,
                                    const aim992_xml_channel_t channels[],
                                    const int selected_indices[],
                                    int selected_count,
                                    aim992_result_t results[]) {
    int fuel_result_idx = -1;
    int steer_result_idx = -1;
    int brake_result_idx = -1;
    const aim992_channel_spec_t *pid_specs[AIM992_MAX_CHANNELS];
    int pid_result_indices[AIM992_MAX_CHANNELS];
    int pid_count = 0;

    for (int i = 0; i < selected_count; i++) {
        int ch_idx = selected_indices[i];
        const aim992_xml_channel_t *xml = &channels[ch_idx];
        const aim992_channel_spec_t *spec = aim992_find_spec_by_name(xml->long_name);

        aim992_result_init(&results[i], xml, spec);

        if (!spec) {
            aim992_copy_cstr(results[i].status, sizeof(results[i].status), "unsupported");
            aim992_copy_cstr(results[i].transport, sizeof(results[i].transport), "unmapped");
            aim992_copy_cstr(results[i].note, sizeof(results[i].note),
                             "XML channel exists but no request mapping is implemented");
            continue;
        }

        if (spec->query_kind == AIM992_QUERY_UDS) {
            aim992_copy_cstr(results[i].transport, sizeof(results[i].transport), "uds");
            if (strcasecmp(xml->long_name, "STEER_ANGLE") == 0) {
                steer_result_idx = i;
            } else if (strcasecmp(xml->long_name, "BRAKE_PRESS") == 0) {
                brake_result_idx = i;
            }
            continue;
        }

        pid_specs[pid_count] = spec;
        pid_result_indices[pid_count] = i;
        pid_count++;
    }

    bool pid_grouped[AIM992_MAX_CHANNELS] = { false };
    for (int i = 0; i < pid_count; i++) {
        if (pid_grouped[i]) continue;
        if (pid_specs[i]->route_count <= 0) continue;

        aim992_route_t route = pid_specs[i]->routes[0];
        if (route.tx_id == 0 || route.rx_id == 0) continue;

        const aim992_channel_spec_t *batch_specs[STN2255_MAX_BATCH_PIDS];
        int batch_result_indices[STN2255_MAX_BATCH_PIDS];
        int batch_count = 0;
        int response_payload_bytes = 1; /* service 0x41 */
        int max_response_payload_bytes = route.tx_id == 0x7E0 ? AIM992_MAX_PAYLOAD_BYTES : 7;

        for (int j = i; j < pid_count && batch_count < STN2255_MAX_BATCH_PIDS; j++) {
            if (pid_grouped[j] || pid_specs[j]->route_count <= 0) continue;
            aim992_route_t candidate = pid_specs[j]->routes[0];
            if (candidate.tx_id != route.tx_id || candidate.rx_id != route.rx_id) continue;

            int add_bytes = aim992_mode01_response_bytes(pid_specs[j]->pid);
            if (response_payload_bytes + add_bytes > max_response_payload_bytes) continue;

            batch_specs[batch_count] = pid_specs[j];
            batch_result_indices[batch_count] = pid_result_indices[j];
            pid_grouped[j] = true;
            batch_count++;
            response_payload_bytes += add_bytes;
        }

        if (batch_count > 1) {
            aim992_query_pid_batch(ctx, batch_specs, batch_result_indices,
                                   batch_count, route, results);
        }
    }

    for (int i = 0; i < pid_count; i++) {
        int result_idx = pid_result_indices[i];
        const aim992_channel_spec_t *spec = pid_specs[i];
        const aim992_xml_channel_t *xml = results[result_idx].xml;

        if (results[result_idx].has_value) {
            continue;
        }

        uint8_t data[ELM327_MAX_DATA_BYTES];
        int data_len = 0;
        char raw_hex[AIM992_MAX_RESPONSE_HEX] = "";

        bool pid_ok = false;
        for (int route_idx = 0; route_idx < spec->route_count; route_idx++) {
            aim992_route_t route = spec->routes[route_idx];
            if (route.tx_id == 0 || route.rx_id == 0) continue;
            if (!aim992_query_pid(ctx, spec->pid, route,
                                  data, &data_len, raw_hex, sizeof(raw_hex))) {
                continue;
            }
            results[result_idx].tx_id = route.tx_id;
            results[result_idx].rx_id = route.rx_id;
            results[result_idx].used_fallback = route_idx > 0;
            aim992_fill_pid_result(&results[result_idx], data, data_len, raw_hex);
            if (route_idx > 0) {
                aim992_copy_cstr(results[result_idx].note, sizeof(results[result_idx].note),
                                 "primary route returned no data; used fallback route");
            }
            pid_ok = true;
            break;
        }

        if (pid_ok) {
            continue;
        }

        if (strcasecmp(xml->long_name, "FUEL_LEVEL") == 0) {
            fuel_result_idx = result_idx;
            aim992_copy_cstr(results[result_idx].status, sizeof(results[result_idx].status), "no_data");
            aim992_copy_cstr(results[result_idx].transport, sizeof(results[result_idx].transport), "mode01");
            aim992_copy_cstr(results[result_idx].note, sizeof(results[result_idx].note),
                             "01 2F returned no data; probing 22 50 75 fallback");
        } else {
            aim992_copy_cstr(results[result_idx].status, sizeof(results[result_idx].status), "no_data");
            aim992_copy_cstr(results[result_idx].transport, sizeof(results[result_idx].transport), "mode01");
        }
    }

    if (steer_result_idx >= 0) {
        static const aim992_route_t grouped_chassis_routes[] = {
            { 0x70C, 0x776 },
        };
        static const aim992_route_t legacy_chassis_routes[] = {
            { 0x70C, 0x73B },
            { 0x7E4, 0x7EC },
            { 0x7E0, 0x7E8 },
            { 0x7E1, 0x7E9 },
        };

        uint8_t payload[AIM992_MAX_PAYLOAD_BYTES];
        int payload_len = 0;
        char raw_hex[AIM992_MAX_RESPONSE_HEX] = "";
        aim992_route_t matched_route = {0};
        char failure_note[160] = "";

        if (aim992_query_did(ctx, 0x5075, grouped_chassis_routes, 1,
                             payload, &payload_len, raw_hex, sizeof(raw_hex), &matched_route,
                             failure_note, sizeof(failure_note))) {
            if (steer_result_idx >= 0) {
                aim992_result_t *res = &results[steer_result_idx];
                res->tx_id = matched_route.tx_id;
                res->rx_id = matched_route.rx_id;
                res->raw_available = true;
                aim992_copy_cstr(res->raw_hex, sizeof(res->raw_hex), raw_hex);
                aim992_copy_cstr(res->transport, sizeof(res->transport), "uds");
                aim992_copy_cstr(res->request, sizeof(res->request), "22 50 75");
                res->heuristic = true;
                if (aim992_decode_steer_5075_guess(res->xml, payload, payload_len,
                                              &res->value, res->note, sizeof(res->note))) {
                    res->has_value = true;
                    aim992_copy_cstr(res->status, sizeof(res->status), "ok");
                } else {
                    aim992_copy_cstr(res->status, sizeof(res->status), "raw_only");
	                    aim992_copy_cstr(res->note, sizeof(res->note),
	                                     "DID 0x5075 responded, but no plausible steering-angle field was found");
	                }
	            }
	        } else if (aim992_query_did(ctx, 0x5101, legacy_chassis_routes, 4,
	                                    payload, &payload_len, raw_hex, sizeof(raw_hex), &matched_route,
	                                    failure_note, sizeof(failure_note))) {
            if (steer_result_idx >= 0) {
                aim992_result_t *res = &results[steer_result_idx];
                res->tx_id = matched_route.tx_id;
                res->rx_id = matched_route.rx_id;
                res->raw_available = true;
                aim992_copy_cstr(res->raw_hex, sizeof(res->raw_hex), raw_hex);
                aim992_copy_cstr(res->transport, sizeof(res->transport), "uds_fallback");
                aim992_copy_cstr(res->request, sizeof(res->request), "22 51 01");
                res->heuristic = true;
                if (aim992_decode_steer_guess(res->xml, payload, payload_len,
                                              &res->value, res->note, sizeof(res->note))) {
                    res->has_value = true;
                    aim992_copy_cstr(res->status, sizeof(res->status), "ok");
                } else {
                    aim992_copy_cstr(res->status, sizeof(res->status), "raw_only");
	                    aim992_copy_cstr(res->note, sizeof(res->note),
	                                     "Fallback DID 0x5101 responded, but no plausible steering-angle field was found");
	                }
	            }
	        } else {
	            const bool negative = failure_note[0] != '\0';
	            const char *note = negative
                ? failure_note
                : "No positive response to grouped DID 0x5075 on 70C->776, and no fallback response to 22 51 01 on legacy routes";
            if (steer_result_idx >= 0) {
                aim992_result_t *res = &results[steer_result_idx];
                if (negative) {
                    res->tx_id = matched_route.tx_id;
                    res->rx_id = matched_route.rx_id;
                    res->raw_available = raw_hex[0] != '\0';
                    aim992_copy_cstr(res->raw_hex, sizeof(res->raw_hex), raw_hex);
                }
                aim992_copy_cstr(results[steer_result_idx].status,
                                 sizeof(results[steer_result_idx].status),
                                 negative ? "negative_response" : "no_data");
                aim992_copy_cstr(results[steer_result_idx].note,
	                                 sizeof(results[steer_result_idx].note),
	                                 note);
	            }
	        }
	    }

    if (brake_result_idx >= 0) {
        static const aim992_route_t brake_routes[] = {
            { 0x713, 0x77D },
        };

        uint8_t payload[AIM992_MAX_PAYLOAD_BYTES];
        int payload_len = 0;
        char raw_hex[AIM992_MAX_RESPONSE_HEX] = "";
        aim992_route_t matched_route = {0};
        char failure_note[160] = "";

        if (aim992_query_did(ctx, 0x2B21, brake_routes, 1,
                             payload, &payload_len, raw_hex, sizeof(raw_hex), &matched_route,
                             failure_note, sizeof(failure_note))) {
            aim992_result_t *res = &results[brake_result_idx];
            res->tx_id = matched_route.tx_id;
            res->rx_id = matched_route.rx_id;
            res->raw_available = true;
            aim992_copy_cstr(res->raw_hex, sizeof(res->raw_hex), raw_hex);
            aim992_copy_cstr(res->transport, sizeof(res->transport), "uds");
            aim992_copy_cstr(res->request, sizeof(res->request), "22 2B 21");
            res->heuristic = true;
            if (aim992_decode_brake_2b21_guess(res->xml, payload, payload_len,
                                               &res->value, res->note, sizeof(res->note))) {
                res->has_value = true;
                aim992_copy_cstr(res->status, sizeof(res->status), "ok");
            } else {
                aim992_copy_cstr(res->status, sizeof(res->status), "raw_only");
                aim992_copy_cstr(res->note, sizeof(res->note),
                                 "DID 0x2B21 responded, but no plausible brake-pressure field was found");
            }
        } else {
            const bool negative = failure_note[0] != '\0';
            aim992_result_t *res = &results[brake_result_idx];
            if (negative) {
                res->tx_id = matched_route.tx_id;
                res->rx_id = matched_route.rx_id;
                res->raw_available = raw_hex[0] != '\0';
                aim992_copy_cstr(res->raw_hex, sizeof(res->raw_hex), raw_hex);
            }
            aim992_copy_cstr(res->status, sizeof(res->status),
                             negative ? "negative_response" : "no_data");
            aim992_copy_cstr(res->note, sizeof(res->note),
                             negative ? failure_note : "No positive response to DID 0x2B21 on 713->77D");
        }
    }

    if (fuel_result_idx >= 0 && !results[fuel_result_idx].has_value) {
        static const aim992_route_t fuel_fallback_routes[] = {
            { 0x7E0, 0x7E8 },
            { 0x7E1, 0x7E9 },
            { 0x7E4, 0x7EC },
        };

        uint8_t payload[AIM992_MAX_PAYLOAD_BYTES];
        int payload_len = 0;
        char raw_hex[AIM992_MAX_RESPONSE_HEX] = "";
        aim992_route_t matched_route = {0};
        char failure_note[160] = "";

        if (aim992_query_did(ctx, 0x5075, fuel_fallback_routes, 3,
                             payload, &payload_len, raw_hex, sizeof(raw_hex), &matched_route,
                             failure_note, sizeof(failure_note))) {
            aim992_result_t *res = &results[fuel_result_idx];
            res->tx_id = matched_route.tx_id;
            res->rx_id = matched_route.rx_id;
            res->raw_available = true;
            res->used_fallback = true;
            aim992_copy_cstr(res->raw_hex, sizeof(res->raw_hex), raw_hex);
            aim992_copy_cstr(res->transport, sizeof(res->transport), "uds_fallback");
            aim992_copy_cstr(res->request, sizeof(res->request), "22 50 75");
            aim992_copy_cstr(res->status, sizeof(res->status), "raw_only");
            aim992_copy_cstr(res->note, sizeof(res->note),
                             "01 2F returned no data; DID 0x5075 responded but scaling is not confirmed yet");
        } else if (failure_note[0]) {
            aim992_result_t *res = &results[fuel_result_idx];
            res->tx_id = matched_route.tx_id;
            res->rx_id = matched_route.rx_id;
            res->raw_available = raw_hex[0] != '\0';
            res->used_fallback = true;
            aim992_copy_cstr(res->raw_hex, sizeof(res->raw_hex), raw_hex);
            aim992_copy_cstr(res->transport, sizeof(res->transport), "uds_fallback");
            aim992_copy_cstr(res->request, sizeof(res->request), "22 50 75");
            aim992_copy_cstr(res->status, sizeof(res->status), "negative_response");
            aim992_copy_cstr(res->note, sizeof(res->note), failure_note);
        }
    }

    return true;
}

static int cli_p992_list(void) {
    aim992_xml_channel_t channels[AIM992_MAX_CHANNELS];
    int channel_count = 0;
    char xml_path[PATH_MAX];

    if (!aim992_load_xml_channels(channels, AIM992_MAX_CHANNELS, &channel_count,
                                  xml_path, sizeof(xml_path))) {
        fprintf(stderr, "[ERROR] Failed to load 992 XML profile\n");
        return 1;
    }

    printf("{\"profile\":\"p992\",\"xml_path\":");
    json_print_string(stdout, xml_path);
    printf(",\"channels\":[\n");
    for (int i = 0; i < channel_count; i++) {
        if (i > 0) printf(",\n");
        aim992_print_list_item(stdout, &channels[i],
                               aim992_find_spec_by_name(channels[i].long_name));
    }
    if (channel_count > 0) printf("\n");
    printf("]}\n");
    return 0;
}

static int cli_p992_read(const char *device, int name_argc, const char *name_argv[]) {
    aim992_xml_channel_t channels[AIM992_MAX_CHANNELS];
    int selected_indices[AIM992_MAX_CHANNELS];
    aim992_result_t results[AIM992_MAX_CHANNELS];
    int channel_count = 0;
    char xml_path[PATH_MAX];

    if (!aim992_load_xml_channels(channels, AIM992_MAX_CHANNELS, &channel_count,
                                  xml_path, sizeof(xml_path))) {
        fprintf(stderr, "[ERROR] Failed to load 992 XML profile\n");
        return 1;
    }

    int selected_count = aim992_select_channels(channels, channel_count,
                                                name_argc, name_argv,
                                                selected_indices, AIM992_MAX_CHANNELS);
    if (selected_count < 0) {
        fprintf(stderr, "[ERROR] Unknown 992 channel name\n");
        return 1;
    }

    int matchIdx = cli_auto_connect(device);
    aim992_query_ctx_t ctx = {0};

    if (!aim992_collect_snapshot(&ctx, channels, selected_indices, selected_count, results)) {
        fprintf(stderr, "[ERROR] Failed to collect 992 snapshot\n");
        [g_ble disconnect];
        return 1;
    }

    printf("{\"profile\":\"p992\",\"xml_path\":");
    json_print_string(stdout, xml_path);
    printf(",\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"results\":[\n");
    for (int i = 0; i < selected_count; i++) {
        if (i > 0) printf(",\n");
        aim992_print_result_item(stdout, &results[i]);
    }
    if (selected_count > 0) printf("\n");
    printf("]}\n");

    [g_ble disconnect];
    return 0;
}

static int cli_p992_watch(const char *device,
                          int interval_ms,
                          int count,
                          int name_argc,
                          const char *name_argv[]) {
    aim992_xml_channel_t channels[AIM992_MAX_CHANNELS];
    int selected_indices[AIM992_MAX_CHANNELS];
    aim992_result_t results[AIM992_MAX_CHANNELS];
    int channel_count = 0;
    char xml_path[PATH_MAX];

    if (!aim992_load_xml_channels(channels, AIM992_MAX_CHANNELS, &channel_count,
                                  xml_path, sizeof(xml_path))) {
        fprintf(stderr, "[ERROR] Failed to load 992 XML profile\n");
        return 1;
    }

    int selected_count = aim992_select_channels(channels, channel_count,
                                                name_argc, name_argv,
                                                selected_indices, AIM992_MAX_CHANNELS);
    if (selected_count < 0) {
        fprintf(stderr, "[ERROR] Unknown 992 channel name\n");
        return 1;
    }

    int matchIdx = cli_auto_connect(device);
    aim992_query_ctx_t ctx = {0};
    int seq = 0;
    long long next_start_ms = get_timestamp_ms();

    while (g_ble.isConnected && !g_interrupt && (count <= 0 || seq < count)) {
        if (!aim992_collect_snapshot(&ctx, channels, selected_indices, selected_count, results)) {
            fprintf(stderr, "[ERROR] Failed to collect 992 snapshot\n");
            [g_ble disconnect];
            return 1;
        }

        printf("{\"profile\":\"p992\",\"xml_path\":");
        json_print_string(stdout, xml_path);
        printf(",\"device\":");
        json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
        printf(",\"sequence\":%d,\"timestamp_ms\":%lld,\"results\":[",
               seq + 1, (long long)get_timestamp_ms());
        for (int i = 0; i < selected_count; i++) {
            if (i > 0) printf(",");
            aim992_print_result_item(stdout, &results[i]);
        }
        printf("]}\n");
        fflush(stdout);

        seq++;
        if (count > 0 && seq >= count) break;

        next_start_ms += interval_ms;
        long long now_ms = get_timestamp_ms();
        if (interval_ms > 0 && next_start_ms > now_ms) {
            run_loop_for((next_start_ms - now_ms) / 1000.0);
        } else {
            next_start_ms = now_ms;
        }
    }

    [g_ble disconnect];
    g_interrupt = NO;
    return 0;
}

static void aim992_debug_command(bool *first, const char *stage, const char *cmd) {
    char resp[1024] = "";
    char hex[AIM992_MAX_RESPONSE_HEX] = "";
    bool received = [g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)];
    bool error = received && elm327_is_error_response(resp);

    if (received && !error) {
        elm327_extract_hex(resp, hex, sizeof(hex));
    }

    if (!*first) printf(",\n");
    *first = false;

    printf("  {\"stage\":");
    json_print_string(stdout, stage);
    printf(",\"cmd\":");
    json_print_string(stdout, cmd);
    printf(",\"received\":%s,\"ok\":%s,\"error\":%s",
           received ? "true" : "false",
           (received && !error) ? "true" : "false",
           error ? "true" : "false");
    printf(",\"resp\":");
    if (received) json_print_string(stdout, resp);
    else printf("null");
    printf(",\"hex\":");
    if (hex[0]) json_print_string(stdout, hex);
    else printf("null");
    printf("}");
}

static int cli_p992_debug(const char *device) {
    int matchIdx = cli_auto_connect(device);
    bool first = true;

    printf("{\"profile\":\"p992\",\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"steps\":[\n");

    aim992_debug_command(&first, "setup", "ATSP6");
    aim992_debug_command(&first, "setup", "ATAT1");
    aim992_debug_command(&first, "setup", "ATST FF");
    aim992_debug_command(&first, "setup", "ATAL");
    aim992_debug_command(&first, "setup", "ATS1");
    aim992_debug_command(&first, "setup", "ATH1");
    aim992_debug_command(&first, "setup", "ATCAF1");

    aim992_debug_command(&first, "mode01_functional_route", "ATSH 7DF");
    aim992_debug_command(&first, "mode01_functional_route", "ATCRA 7E8");
    aim992_debug_command(&first, "mode01_functional_route", "0100");
    aim992_debug_command(&first, "mode01_functional_route", "010C");
    aim992_debug_command(&first, "mode01_functional_route", "0111");
    aim992_debug_command(&first, "mode01_functional_route", "0149");
    aim992_debug_command(&first, "mode01_functional_route", "012F");

    aim992_debug_command(&first, "mode01_physical_route", "ATSH 7E0");
    aim992_debug_command(&first, "mode01_physical_route", "ATCRA 7E8");
    aim992_debug_command(&first, "mode01_physical_route", "0100");
    aim992_debug_command(&first, "mode01_physical_route", "010C");
    aim992_debug_command(&first, "mode01_physical_route", "0111");
    aim992_debug_command(&first, "mode01_physical_route", "0149");
    aim992_debug_command(&first, "mode01_physical_route", "012F");
    aim992_debug_command(&first, "uds_7e0_7e8", "225075");
    aim992_debug_command(&first, "uds_7e0_7e8", "225101");

    aim992_debug_command(&first, "uds_7e0_extended_session", "1003");
    aim992_debug_command(&first, "uds_7e0_extended_session", "3E00");
    aim992_debug_command(&first, "uds_7e0_extended_session", "225075");
    aim992_debug_command(&first, "uds_7e0_extended_session", "225101");

    aim992_debug_command(&first, "uds_70c_714", "ATSH 70C");
    aim992_debug_command(&first, "uds_70c_714", "ATCRA 714");
    aim992_debug_command(&first, "uds_70c_714", "225101");

    aim992_debug_command(&first, "uds_70c_73b", "ATSH 70C");
    aim992_debug_command(&first, "uds_70c_73b", "ATCRA 73B");
    aim992_debug_command(&first, "uds_70c_73b", "225101");

    aim992_debug_command(&first, "uds_70c_auto_receive", "ATAR");
    aim992_debug_command(&first, "uds_70c_auto_receive", "ATSH 70C");
    aim992_debug_command(&first, "uds_70c_auto_receive", "225101");

    aim992_debug_command(&first, "uds_70c_extended_session", "1003");
    aim992_debug_command(&first, "uds_70c_extended_session", "3E00");
    aim992_debug_command(&first, "uds_70c_extended_session", "225101");

    aim992_debug_command(&first, "uds_73b_70c", "ATSH 73B");
    aim992_debug_command(&first, "uds_73b_70c", "ATCRA 70C");
    aim992_debug_command(&first, "uds_73b_70c", "225101");

    aim992_debug_command(&first, "uds_73b_auto_receive", "ATAR");
    aim992_debug_command(&first, "uds_73b_auto_receive", "ATSH 73B");
    aim992_debug_command(&first, "uds_73b_auto_receive", "225101");

    aim992_debug_command(&first, "bin_raw_container_setup", "ATCAF0");
    aim992_debug_command(&first, "bin_raw_container_7e0_7e8", "ATSH 7E0");
    aim992_debug_command(&first, "bin_raw_container_7e0_7e8", "ATCRA 7E8");
    aim992_debug_command(&first, "bin_raw_container_7e0_7e8", "0322507500000000");
    aim992_debug_command(&first, "bin_raw_container_7e0_7e8", "0322510100000000");

    aim992_debug_command(&first, "bin_raw_container_70c_714", "ATSH 70C");
    aim992_debug_command(&first, "bin_raw_container_70c_714", "ATCRA 714");
    aim992_debug_command(&first, "bin_raw_container_70c_714", "0322510100000000");

    aim992_debug_command(&first, "bin_raw_container_70c_73b", "ATSH 70C");
    aim992_debug_command(&first, "bin_raw_container_70c_73b", "ATCRA 73B");
    aim992_debug_command(&first, "bin_raw_container_70c_73b", "0322510100000000");

    aim992_debug_command(&first, "bin_raw_container_70c_auto_receive", "ATAR");
    aim992_debug_command(&first, "bin_raw_container_70c_auto_receive", "ATSH 70C");
    aim992_debug_command(&first, "bin_raw_container_70c_auto_receive", "0322510100000000");

    aim992_debug_command(&first, "bin_raw_container_73b_70c", "ATSH 73B");
    aim992_debug_command(&first, "bin_raw_container_73b_70c", "ATCRA 70C");
    aim992_debug_command(&first, "bin_raw_container_73b_70c", "0322510100000000");

    aim992_debug_command(&first, "bin_raw_container_73b_auto_receive", "ATAR");
    aim992_debug_command(&first, "bin_raw_container_73b_auto_receive", "ATSH 73B");
    aim992_debug_command(&first, "bin_raw_container_73b_auto_receive", "0322510100000000");

    printf("\n]}\n");

    [g_ble disconnect];
    return 0;
}

static int cli_p992_bin_probe(const char *device) {
    int matchIdx = cli_auto_connect(device);
    bool first = true;

    printf("{\"profile\":\"p992\",\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"probe\":\"bin_tail_exact\",\"steps\":[\n");

    aim992_debug_command(&first, "setup", "ATSP6");
    aim992_debug_command(&first, "setup", "ATAT0");
    aim992_debug_command(&first, "setup", "ATST FF");
    aim992_debug_command(&first, "setup", "ATAL");
    aim992_debug_command(&first, "setup", "ATS1");
    aim992_debug_command(&first, "setup", "ATH1");

    aim992_debug_command(&first, "caf1_flow_control_70c_73b", "ATCAF1");
    aim992_debug_command(&first, "caf1_flow_control_70c_73b", "ATFCSH70C");
    aim992_debug_command(&first, "caf1_flow_control_70c_73b", "ATFCSD300002");
    aim992_debug_command(&first, "caf1_flow_control_70c_73b", "ATFCSM1");
    aim992_debug_command(&first, "caf1_flow_control_70c_73b", "ATSH 70C");
    aim992_debug_command(&first, "caf1_flow_control_70c_73b", "ATCRA 73B");
    aim992_debug_command(&first, "caf1_flow_control_70c_73b", "225101");

    aim992_debug_command(&first, "caf1_auto_receive_70c", "ATAR");
    aim992_debug_command(&first, "caf1_auto_receive_70c", "ATSH 70C");
    aim992_debug_command(&first, "caf1_auto_receive_70c", "225101");

    aim992_debug_command(&first, "caf1_extended_addr_70c_73b", "ATCEA 00");
    aim992_debug_command(&first, "caf1_extended_addr_70c_73b", "ATCAF1");
    aim992_debug_command(&first, "caf1_extended_addr_70c_73b", "ATSH 70C");
    aim992_debug_command(&first, "caf1_extended_addr_70c_73b", "ATCRA 73B");
    aim992_debug_command(&first, "caf1_extended_addr_70c_73b", "225101");
    aim992_debug_command(&first, "caf1_extended_addr_70c_auto", "ATAR");
    aim992_debug_command(&first, "caf1_extended_addr_70c_auto", "ATSH 70C");
    aim992_debug_command(&first, "caf1_extended_addr_70c_auto", "225101");
    aim992_debug_command(&first, "caf1_extended_addr_off", "ATCEA");

    aim992_debug_command(&first, "caf0_bin_padding_70c_73b", "ATCAF0");
    aim992_debug_command(&first, "caf0_bin_padding_70c_73b", "ATSH 70C");
    aim992_debug_command(&first, "caf0_bin_padding_70c_73b", "ATCRA 73B");
    aim992_debug_command(&first, "caf0_bin_padding_70c_73b", "03225101AAAAAAAA");
    aim992_debug_command(&first, "caf0_bin_padding_70c_73b", "0003225101AAAAAA");
    aim992_debug_command(&first, "caf0_bin_padding_70c_73b", "0322510100000000");

    aim992_debug_command(&first, "caf0_bin_padding_70c_auto", "ATAR");
    aim992_debug_command(&first, "caf0_bin_padding_70c_auto", "ATSH 70C");
    aim992_debug_command(&first, "caf0_bin_padding_70c_auto", "03225101AAAAAAAA");
    aim992_debug_command(&first, "caf0_bin_padding_70c_auto", "0003225101AAAAAA");

    aim992_debug_command(&first, "caf0_bin_padding_7e0_7e8", "ATSH 7E0");
    aim992_debug_command(&first, "caf0_bin_padding_7e0_7e8", "ATCRA 7E8");
    aim992_debug_command(&first, "caf0_bin_padding_7e0_7e8", "03225075AAAAAAAA");
    aim992_debug_command(&first, "caf0_bin_padding_7e0_7e8", "03225101AAAAAAAA");

    aim992_debug_command(&first, "hybrid_route_7e0_73b", "ATCAF1");
    aim992_debug_command(&first, "hybrid_route_7e0_73b", "ATSH 7E0");
    aim992_debug_command(&first, "hybrid_route_7e0_73b", "ATCRA 73B");
    aim992_debug_command(&first, "hybrid_route_7e0_73b", "225075");
    aim992_debug_command(&first, "hybrid_route_7e0_73b", "225101");

    aim992_debug_command(&first, "hybrid_route_70c_73b", "ATCAF1");
    aim992_debug_command(&first, "hybrid_route_70c_73b", "ATSH 70C");
    aim992_debug_command(&first, "hybrid_route_70c_73b", "ATCRA 73B");
    aim992_debug_command(&first, "hybrid_route_70c_73b", "225075");
    aim992_debug_command(&first, "hybrid_route_70c_73b", "225101");

    aim992_debug_command(&first, "caf0_hybrid_route_7e0_73b", "ATCAF0");
    aim992_debug_command(&first, "caf0_hybrid_route_7e0_73b", "ATSH 7E0");
    aim992_debug_command(&first, "caf0_hybrid_route_7e0_73b", "ATCRA 73B");
    aim992_debug_command(&first, "caf0_hybrid_route_7e0_73b", "03225075AAAAAAAA");
    aim992_debug_command(&first, "caf0_hybrid_route_7e0_73b", "03225101AAAAAAAA");

    printf("\n]}\n");

    [g_ble disconnect];
    return 0;
}

static int cli_p992_scan_did(const char *device, uint16_t did, int session) {
    int matchIdx = cli_auto_connect(device);
    char resp[512];
    bool first = true;

    [g_ble sendCommand:"ATSP6" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAT0" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATST 10" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAL" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATS1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATH1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATCAF1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAR" response:resp maxLen:sizeof(resp)];

    printf("{\"profile\":\"p992\",\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"did\":\"%04X\"", did);
    if (session >= 0) printf(",\"session\":\"%02X\"", session & 0xFF);
    printf(",\"responders\":[\n");

    for (uint32_t tx = 0x700; tx <= 0x7EF; tx++) {
        char cmd[32];
        char query[16];
        char hex[AIM992_MAX_RESPONSE_HEX] = "";
        char session_resp[512] = "";
        char session_hex[AIM992_MAX_RESPONSE_HEX] = "";
        bool session_received = false;
        bool session_error = false;
        bool session_positive = false;
        bool session_negative = false;
        uint8_t session_nrc = 0;

        snprintf(cmd, sizeof(cmd), "ATSH %03X", tx);
        if (![g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)] ||
            elm327_is_error_response(resp)) {
            continue;
        }

        if (session >= 0) {
            char session_cmd[16];
            snprintf(session_cmd, sizeof(session_cmd), "10%02X", session & 0xFF);
            session_received = [g_ble sendCommand:session_cmd
                                         response:session_resp
                                           maxLen:sizeof(session_resp)];
            session_error = session_received && elm327_is_error_response(session_resp);
            if (session_received && !session_error) {
                elm327_extract_hex(session_resp, session_hex, sizeof(session_hex));
                session_positive =
                    aim992_find_uds_positive_session_response(session_hex, (uint8_t)session);
                session_negative =
                    aim992_find_uds_negative_response(session_hex, 0x10, &session_nrc);
            }
        }

        snprintf(query, sizeof(query), "22%02X%02X", did >> 8, did & 0xFF);
        if (![g_ble sendCommand:query response:resp maxLen:sizeof(resp)] ||
            elm327_is_error_response(resp)) {
            continue;
        }

        elm327_extract_hex(resp, hex, sizeof(hex));
        bool positive = aim992_find_uds_positive_did_response(hex, did);
        uint8_t nrc = 0;
        bool negative = aim992_find_uds_negative_response(hex, 0x22, &nrc);
        if (!first) printf(",\n");
        first = false;

        printf("  {\"tx_id\":\"%03X\",\"resp\":", tx);
        json_print_string(stdout, resp);
        printf(",\"hex\":");
        if (hex[0]) json_print_string(stdout, hex);
        else printf("null");
        if (session >= 0) {
            printf(",\"session_resp\":");
            if (session_received) json_print_string(stdout, session_resp);
            else printf("null");
            printf(",\"session_hex\":");
            if (session_hex[0]) json_print_string(stdout, session_hex);
            else printf("null");
            printf(",\"session_status\":");
            json_print_string(stdout,
                              session_positive ? "positive" :
                              (session_negative ? "negative" :
                               (session_error ? "adapter_error" : "unknown")));
            if (session_negative) {
                printf(",\"session_nrc\":\"%02X\",\"session_nrc_name\":", session_nrc);
                json_print_string(stdout, aim992_uds_nrc_name(session_nrc));
            }
        }
        printf(",\"uds_status\":");
        json_print_string(stdout, positive ? "positive" : (negative ? "negative" : "unknown"));
        printf(",\"positive\":%s", positive ? "true" : "false");
        if (negative) {
            printf(",\"nrc\":\"%02X\",\"nrc_name\":", nrc);
            json_print_string(stdout, aim992_uds_nrc_name(nrc));
        }
        printf("}");
        fflush(stdout);
    }

    if (!first) printf("\n");
    printf("]}\n");

    [g_ble disconnect];
    return 0;
}

static int cli_p992_scan_did_range(const char *device, uint16_t start_did, uint16_t end_did) {
    static const aim992_route_t routes[] = {
        { 0x7E0, 0x7E8 },
        { 0x7E1, 0x7E9 },
        { 0x7E4, 0x7EC },
    };

    if (end_did < start_did) {
        uint16_t tmp = start_did;
        start_did = end_did;
        end_did = tmp;
    }

    int matchIdx = cli_auto_connect(device);
    char resp[512];
    bool first = true;

    [g_ble sendCommand:"ATSP6" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAT0" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATST 10" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAL" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATS1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATH1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATCAF1" response:resp maxLen:sizeof(resp)];

    printf("{\"profile\":\"p992\",\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"start_did\":\"%04X\",\"end_did\":\"%04X\",\"responses\":[\n",
           start_did, end_did);

    for (uint32_t did = start_did; did <= end_did; did++) {
        for (size_t i = 0; i < sizeof(routes) / sizeof(routes[0]); i++) {
            char cmd[32];
            char query[16];
            char hex[AIM992_MAX_RESPONSE_HEX] = "";

            snprintf(cmd, sizeof(cmd), "ATSH %03X", routes[i].tx_id);
            if (![g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)] ||
                elm327_is_error_response(resp)) {
                continue;
            }

            snprintf(cmd, sizeof(cmd), "ATCRA %03X", routes[i].rx_id);
            if (![g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)] ||
                elm327_is_error_response(resp)) {
                continue;
            }

            snprintf(query, sizeof(query), "22%02X%02X", did >> 8, did & 0xFF);
            if (![g_ble sendCommand:query response:resp maxLen:sizeof(resp)] ||
                elm327_is_error_response(resp)) {
                continue;
            }

            elm327_extract_hex(resp, hex, sizeof(hex));
            bool positive = aim992_find_uds_positive_did_response(hex, (uint16_t)did);
            uint8_t nrc = 0;
            bool negative = aim992_find_uds_negative_response(hex, 0x22, &nrc);

            if (!positive && !negative) continue;

            if (!first) printf(",\n");
            first = false;

            printf("  {\"did\":\"%04X\",\"tx_id\":\"%03X\",\"rx_id\":\"%03X\",\"resp\":",
                   (unsigned)did, routes[i].tx_id, routes[i].rx_id);
            json_print_string(stdout, resp);
            printf(",\"hex\":");
            if (hex[0]) json_print_string(stdout, hex);
            else printf("null");
            printf(",\"uds_status\":");
            json_print_string(stdout, positive ? "positive" : "negative");
            printf(",\"positive\":%s", positive ? "true" : "false");
            if (negative) {
                printf(",\"nrc\":\"%02X\",\"nrc_name\":", nrc);
                json_print_string(stdout, aim992_uds_nrc_name(nrc));
            }
            printf("}");
            fflush(stdout);
        }

        if (did == 0xFFFF) break;
    }

    if (!first) printf("\n");
    printf("]}\n");

    [g_ble disconnect];
    return 0;
}

static int cli_p992_uds_read(const char *device,
                             uint16_t did,
                             uint32_t tx_id,
                             int rx_id,
                             int session,
                             const char *pre_request_hex) {
    int matchIdx = cli_auto_connect(device);
    char resp[512];
    char cmd[32];

    [g_ble sendCommand:"ATSP6" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAT0" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATST FF" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAL" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATS1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATH1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATCAF1" response:resp maxLen:sizeof(resp)];

    snprintf(cmd, sizeof(cmd), "ATSH %03X", tx_id);
    [g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)];

    if (rx_id >= 0) {
        snprintf(cmd, sizeof(cmd), "ATCRA %03X", rx_id);
        [g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)];
    } else {
        [g_ble sendCommand:"ATAR" response:resp maxLen:sizeof(resp)];
    }

    aim992_route_t uds_route = { tx_id, rx_id >= 0 ? (uint32_t)rx_id : UINT32_MAX };
    aim992_configure_flow_control(uds_route);

    char session_resp[512] = "";
    char session_hex[AIM992_MAX_RESPONSE_HEX] = "";
    char session_payload_hex[AIM992_MAX_RESPONSE_HEX] = "";
    bool session_received = false;
    bool session_error = false;
    bool session_positive = false;
    bool session_negative = false;
    uint8_t session_nrc = 0;
    uint8_t session_payload[AIM992_MAX_PAYLOAD_BYTES];
    int session_payload_len = 0;
    uint32_t session_resp_id = UINT32_MAX;

    if (session >= 0) {
        snprintf(cmd, sizeof(cmd), "10%02X", session & 0xFF);
        session_received = [g_ble sendCommand:cmd
                                     response:session_resp
                                       maxLen:sizeof(session_resp)];
        session_error = session_received && elm327_is_error_response(session_resp);
        if (session_received && !session_error) {
            elm327_extract_hex(session_resp, session_hex, sizeof(session_hex));
            if (aim992_parse_isotp_payload(session_resp, session_payload,
                                           AIM992_MAX_PAYLOAD_BYTES,
                                           &session_payload_len,
                                           &session_resp_id)) {
                aim992_format_payload_hex(session_payload, session_payload_len,
                                          session_payload_hex, sizeof(session_payload_hex));
                session_positive =
                    session_payload_len >= 2 &&
                    session_payload[0] == 0x50 &&
                    session_payload[1] == (uint8_t)session;
                session_negative =
                    session_payload_len >= 3 &&
                    session_payload[0] == 0x7F &&
                    session_payload[1] == 0x10;
                if (session_negative) session_nrc = session_payload[2];
            } else {
                session_positive =
                    aim992_find_uds_positive_session_response(session_hex, (uint8_t)session);
                session_negative =
                    aim992_find_uds_negative_response(session_hex, 0x10, &session_nrc);
            }
        }
    }

    char query_resp[1024] = "";
    char query_hex[AIM992_MAX_RESPONSE_HEX] = "";
    char query_payload_hex[AIM992_MAX_RESPONSE_HEX] = "";
    char text[128] = "";
    char pre_resp[512] = "";
    char pre_hex[AIM992_MAX_RESPONSE_HEX] = "";
    char pre_payload_hex[AIM992_MAX_RESPONSE_HEX] = "";
    bool query_received = false;
    bool query_error = false;
    bool positive = false;
    bool negative = false;
    bool pre_received = false;
    bool pre_error = false;
    bool pre_positive = false;
    bool pre_negative = false;
    uint8_t nrc = 0;
    uint8_t pre_nrc = 0;
    uint8_t query_payload[128];
    uint8_t pre_payload[128];
    int query_payload_len = 0;
    int pre_payload_len = 0;
    uint32_t query_resp_id = UINT32_MAX;
    uint32_t pre_resp_id = UINT32_MAX;

    if (pre_request_hex && pre_request_hex[0]) {
        uint8_t pre_service_id = 0;
        if (strlen(pre_request_hex) >= 2 &&
            (strlen(pre_request_hex) % 2) == 0 &&
            aim992_parse_hex_byte(pre_request_hex, &pre_service_id)) {
            snprintf(cmd, sizeof(cmd), "%s", pre_request_hex);
            pre_received = [g_ble sendCommand:cmd response:pre_resp maxLen:sizeof(pre_resp)];
            pre_error = pre_received && elm327_is_error_response(pre_resp);
            if (pre_received && !pre_error) {
                elm327_extract_hex(pre_resp, pre_hex, sizeof(pre_hex));
                if (aim992_parse_isotp_payload(pre_resp, pre_payload, sizeof(pre_payload),
                                               &pre_payload_len, &pre_resp_id)) {
                    aim992_format_payload_hex(pre_payload, pre_payload_len,
                                              pre_payload_hex, sizeof(pre_payload_hex));
                    pre_positive =
                        pre_payload_len >= 1 &&
                        pre_payload[0] == (uint8_t)(pre_service_id + 0x40);
                    pre_negative =
                        pre_payload_len >= 3 &&
                        pre_payload[0] == 0x7F &&
                        pre_payload[1] == pre_service_id;
                    if (pre_negative) pre_nrc = pre_payload[2];
                } else {
                    char positive_marker[8];
                    snprintf(positive_marker, sizeof(positive_marker), "%02X",
                             pre_service_id + 0x40);
                    pre_positive = strstr(pre_hex, positive_marker) != NULL;
                    pre_negative =
                        aim992_find_uds_negative_response(pre_hex, pre_service_id, &pre_nrc);
                }
            }
        }
    }

    snprintf(cmd, sizeof(cmd), "22%02X%02X", did >> 8, did & 0xFF);
    query_received = [g_ble sendCommand:cmd response:query_resp maxLen:sizeof(query_resp)];
    query_error = query_received && elm327_is_error_response(query_resp);
    if (query_received && !query_error) {
        elm327_extract_hex(query_resp, query_hex, sizeof(query_hex));
        if (aim992_parse_isotp_payload(query_resp, query_payload, sizeof(query_payload),
                                       &query_payload_len, &query_resp_id)) {
            aim992_format_payload_hex(query_payload, query_payload_len,
                                      query_payload_hex, sizeof(query_payload_hex));
            positive =
                query_payload_len >= 3 &&
                query_payload[0] == 0x62 &&
                query_payload[1] == (uint8_t)(did >> 8) &&
                query_payload[2] == (uint8_t)(did & 0xFF);
            negative =
                query_payload_len >= 3 &&
                query_payload[0] == 0x7F &&
                query_payload[1] == 0x22;
            if (negative) nrc = query_payload[2];
            if (positive) {
                aim992_payload_ascii(query_payload, query_payload_len, 3,
                                     text, sizeof(text));
            }
        } else {
            positive = aim992_find_uds_positive_did_response(query_hex, did);
            negative = aim992_find_uds_negative_response(query_hex, 0x22, &nrc);
        }
    }

    const char *status = "unknown";
    if (!query_received) status = "timeout";
    else if (query_error) status = strstr(query_resp, "NO DATA") ? "no_data" : "adapter_error";
    else if (positive) status = "positive";
    else if (negative) status = "negative";

    printf("{\"profile\":\"p992\",\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"did\":\"%04X\",\"tx_id\":\"%03X\",\"rx_id\":", did, tx_id);
    if (rx_id >= 0) {
        char rx_id_str[8];
        snprintf(rx_id_str, sizeof(rx_id_str), "%03X", rx_id);
        json_print_string(stdout, rx_id_str);
    } else {
        printf("null");
    }
    if (session >= 0) printf(",\"session\":\"%02X\"", session & 0xFF);
    printf(",\"status\":");
    json_print_string(stdout, status);
    printf(",\"resp\":");
    if (query_received) json_print_string(stdout, query_resp);
    else printf("null");
    printf(",\"hex\":");
    if (query_hex[0]) json_print_string(stdout, query_hex);
    else printf("null");
    printf(",\"resp_id\":");
    if (query_resp_id <= 0x7FF) {
        char resp_id_str[8];
        snprintf(resp_id_str, sizeof(resp_id_str), "%03X", query_resp_id);
        json_print_string(stdout, resp_id_str);
    } else {
        printf("null");
    }
    printf(",\"payload_hex\":");
    if (query_payload_hex[0]) json_print_string(stdout, query_payload_hex);
    else printf("null");
    printf(",\"positive\":%s", positive ? "true" : "false");
    if (negative) {
        printf(",\"nrc\":\"%02X\",\"nrc_name\":", nrc);
        json_print_string(stdout, aim992_uds_nrc_name(nrc));
    }
    printf(",\"text\":");
    if (text[0]) json_print_string(stdout, text);
    else printf("null");
    if (session >= 0) {
        printf(",\"session_resp\":");
        if (session_received) json_print_string(stdout, session_resp);
        else printf("null");
        printf(",\"session_hex\":");
        if (session_hex[0]) json_print_string(stdout, session_hex);
        else printf("null");
        printf(",\"session_resp_id\":");
        if (session_resp_id <= 0x7FF) {
            char session_resp_id_str[8];
            snprintf(session_resp_id_str, sizeof(session_resp_id_str), "%03X", session_resp_id);
            json_print_string(stdout, session_resp_id_str);
        } else {
            printf("null");
        }
        printf(",\"session_payload_hex\":");
        if (session_payload_hex[0]) json_print_string(stdout, session_payload_hex);
        else printf("null");
        printf(",\"session_status\":");
        if (!session_received) json_print_string(stdout, "timeout");
        else if (session_error) json_print_string(stdout, strstr(session_resp, "NO DATA") ? "no_data" : "adapter_error");
        else if (session_positive) json_print_string(stdout, "positive");
        else if (session_negative) json_print_string(stdout, "negative");
        else json_print_string(stdout, "unknown");
        if (session_negative) {
            printf(",\"session_nrc\":\"%02X\",\"session_nrc_name\":", session_nrc);
            json_print_string(stdout, aim992_uds_nrc_name(session_nrc));
        }
    }
    if (pre_request_hex && pre_request_hex[0]) {
        printf(",\"pre_request\":");
        json_print_string(stdout, pre_request_hex);
        printf(",\"pre_resp\":");
        if (pre_received) json_print_string(stdout, pre_resp);
        else printf("null");
        printf(",\"pre_hex\":");
        if (pre_hex[0]) json_print_string(stdout, pre_hex);
        else printf("null");
        printf(",\"pre_resp_id\":");
        if (pre_resp_id <= 0x7FF) {
            char pre_resp_id_str[8];
            snprintf(pre_resp_id_str, sizeof(pre_resp_id_str), "%03X", pre_resp_id);
            json_print_string(stdout, pre_resp_id_str);
        } else {
            printf("null");
        }
        printf(",\"pre_payload_hex\":");
        if (pre_payload_hex[0]) json_print_string(stdout, pre_payload_hex);
        else printf("null");
        printf(",\"pre_status\":");
        if (!pre_received) json_print_string(stdout, "timeout");
        else if (pre_error) json_print_string(stdout, strstr(pre_resp, "NO DATA") ? "no_data" : "adapter_error");
        else if (pre_positive) json_print_string(stdout, "positive");
        else if (pre_negative) json_print_string(stdout, "negative");
        else json_print_string(stdout, "unknown");
        if (pre_negative) {
            printf(",\"pre_nrc\":\"%02X\",\"pre_nrc_name\":", pre_nrc);
            json_print_string(stdout, aim992_uds_nrc_name(pre_nrc));
        }
    }
    printf("}\n");

    [g_ble disconnect];
    return 0;
}

static int cli_p992_uds_raw(const char *device,
                            const char *request_hex,
                            uint32_t tx_id,
                            int rx_id,
                            int session) {
    uint8_t service_id = 0;
    if (!request_hex || strlen(request_hex) < 2 ||
        (strlen(request_hex) % 2) != 0 ||
        !aim992_parse_hex_byte(request_hex, &service_id)) {
        fprintf(stderr, "[ERROR] Invalid UDS request hex: %s\n",
                request_hex ? request_hex : "(null)");
        return 1;
    }

    int matchIdx = cli_auto_connect(device);
    char resp[512];
    char cmd[64];

    [g_ble sendCommand:"ATSP6" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAT0" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATST FF" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATAL" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATS1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATH1" response:resp maxLen:sizeof(resp)];
    [g_ble sendCommand:"ATCAF1" response:resp maxLen:sizeof(resp)];

    snprintf(cmd, sizeof(cmd), "ATSH %03X", tx_id);
    [g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)];

    if (rx_id >= 0) {
        snprintf(cmd, sizeof(cmd), "ATCRA %03X", rx_id);
        [g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)];
    } else {
        [g_ble sendCommand:"ATAR" response:resp maxLen:sizeof(resp)];
    }

    char session_resp[512] = "";
    char session_hex[AIM992_MAX_RESPONSE_HEX] = "";
    char session_payload_hex[AIM992_MAX_RESPONSE_HEX] = "";
    bool session_received = false;
    bool session_error = false;
    bool session_positive = false;
    bool session_negative = false;
    uint8_t session_nrc = 0;
    uint8_t session_payload[AIM992_MAX_PAYLOAD_BYTES];
    int session_payload_len = 0;
    uint32_t session_resp_id = UINT32_MAX;

    if (session >= 0) {
        snprintf(cmd, sizeof(cmd), "10%02X", session & 0xFF);
        session_received = [g_ble sendCommand:cmd
                                     response:session_resp
                                       maxLen:sizeof(session_resp)];
        session_error = session_received && elm327_is_error_response(session_resp);
        if (session_received && !session_error) {
            elm327_extract_hex(session_resp, session_hex, sizeof(session_hex));
            if (aim992_parse_isotp_payload(session_resp, session_payload,
                                           AIM992_MAX_PAYLOAD_BYTES,
                                           &session_payload_len,
                                           &session_resp_id)) {
                aim992_format_payload_hex(session_payload, session_payload_len,
                                          session_payload_hex, sizeof(session_payload_hex));
                session_positive =
                    session_payload_len >= 2 &&
                    session_payload[0] == 0x50 &&
                    session_payload[1] == (uint8_t)session;
                session_negative =
                    session_payload_len >= 3 &&
                    session_payload[0] == 0x7F &&
                    session_payload[1] == 0x10;
                if (session_negative) session_nrc = session_payload[2];
            } else {
                session_positive =
                    aim992_find_uds_positive_session_response(session_hex, (uint8_t)session);
                session_negative =
                    aim992_find_uds_negative_response(session_hex, 0x10, &session_nrc);
            }
        }
    }

    char query_resp[1024] = "";
    char query_hex[AIM992_MAX_RESPONSE_HEX] = "";
    char query_payload_hex[AIM992_MAX_RESPONSE_HEX] = "";
    char text[128] = "";
    bool query_received = false;
    bool query_error = false;
    bool positive = false;
    bool negative = false;
    uint8_t nrc = 0;
    uint8_t query_payload[128];
    int query_payload_len = 0;
    uint32_t query_resp_id = UINT32_MAX;

    aim992_copy_cstr(cmd, sizeof(cmd), request_hex);
    query_received = [g_ble sendCommand:cmd response:query_resp maxLen:sizeof(query_resp)];
    query_error = query_received && elm327_is_error_response(query_resp);
    if (query_received && !query_error) {
        elm327_extract_hex(query_resp, query_hex, sizeof(query_hex));
        if (aim992_parse_isotp_payload(query_resp, query_payload, sizeof(query_payload),
                                       &query_payload_len, &query_resp_id)) {
            aim992_format_payload_hex(query_payload, query_payload_len,
                                      query_payload_hex, sizeof(query_payload_hex));
            positive =
                query_payload_len >= 1 &&
                query_payload[0] == (uint8_t)(service_id + 0x40);
            negative =
                query_payload_len >= 3 &&
                query_payload[0] == 0x7F &&
                query_payload[1] == service_id;
            if (negative) nrc = query_payload[2];
            if (positive) {
                aim992_payload_ascii(query_payload, query_payload_len, 1,
                                     text, sizeof(text));
            }
        } else {
            char positive_marker[8];
            snprintf(positive_marker, sizeof(positive_marker), "%02X", service_id + 0x40);
            positive = strstr(query_hex, positive_marker) != NULL;
            negative = aim992_find_uds_negative_response(query_hex, service_id, &nrc);
        }
    }

    const char *status = "unknown";
    if (!query_received) status = "timeout";
    else if (query_error) status = strstr(query_resp, "NO DATA") ? "no_data" : "adapter_error";
    else if (positive) status = "positive";
    else if (negative) status = "negative";

    printf("{\"profile\":\"p992\",\"device\":");
    json_print_string(stdout, g_ble.discoveredNames[matchIdx].UTF8String);
    printf(",\"request\":");
    json_print_string(stdout, request_hex);
    printf(",\"service_id\":\"%02X\",\"tx_id\":\"%03X\",\"rx_id\":", service_id, tx_id);
    if (rx_id >= 0) {
        char rx_id_str[8];
        snprintf(rx_id_str, sizeof(rx_id_str), "%03X", rx_id);
        json_print_string(stdout, rx_id_str);
    } else {
        printf("null");
    }
    if (session >= 0) printf(",\"session\":\"%02X\"", session & 0xFF);
    printf(",\"status\":");
    json_print_string(stdout, status);
    printf(",\"resp\":");
    if (query_received) json_print_string(stdout, query_resp);
    else printf("null");
    printf(",\"hex\":");
    if (query_hex[0]) json_print_string(stdout, query_hex);
    else printf("null");
    printf(",\"resp_id\":");
    if (query_resp_id <= 0x7FF) {
        char resp_id_str[8];
        snprintf(resp_id_str, sizeof(resp_id_str), "%03X", query_resp_id);
        json_print_string(stdout, resp_id_str);
    } else {
        printf("null");
    }
    printf(",\"payload_hex\":");
    if (query_payload_hex[0]) json_print_string(stdout, query_payload_hex);
    else printf("null");
    printf(",\"positive\":%s", positive ? "true" : "false");
    if (negative) {
        printf(",\"nrc\":\"%02X\",\"nrc_name\":", nrc);
        json_print_string(stdout, aim992_uds_nrc_name(nrc));
    }
    printf(",\"text\":");
    if (text[0]) json_print_string(stdout, text);
    else printf("null");
    if (session >= 0) {
        printf(",\"session_resp\":");
        if (session_received) json_print_string(stdout, session_resp);
        else printf("null");
        printf(",\"session_hex\":");
        if (session_hex[0]) json_print_string(stdout, session_hex);
        else printf("null");
        printf(",\"session_resp_id\":");
        if (session_resp_id <= 0x7FF) {
            char session_resp_id_str[8];
            snprintf(session_resp_id_str, sizeof(session_resp_id_str), "%03X", session_resp_id);
            json_print_string(stdout, session_resp_id_str);
        } else {
            printf("null");
        }
        printf(",\"session_payload_hex\":");
        if (session_payload_hex[0]) json_print_string(stdout, session_payload_hex);
        else printf("null");
        printf(",\"session_status\":");
        if (!session_received) json_print_string(stdout, "timeout");
        else if (session_error) json_print_string(stdout, strstr(session_resp, "NO DATA") ? "no_data" : "adapter_error");
        else if (session_positive) json_print_string(stdout, "positive");
        else if (session_negative) json_print_string(stdout, "negative");
        else json_print_string(stdout, "unknown");
        if (session_negative) {
            printf(",\"session_nrc\":\"%02X\",\"session_nrc_name\":", session_nrc);
            json_print_string(stdout, aim992_uds_nrc_name(session_nrc));
        }
    }
    printf("}\n");

    [g_ble disconnect];
    return 0;
}

typedef struct {
    const char *name;
    uint32_t    tx_id;
    const char *payload_hex;
} aim992_fastlog_request_t;

static const aim992_fastlog_request_t s_aim992_fastlog_requests[] = {
    { "RPM",          0x7DF, "02010C5555555555" },
    { "THROTTLE_POS", 0x7DF, "0201115555555555" },
    { "ACCEL_POS",    0x7DF, "0201495555555555" },
    { "STEER_ANGLE",  0x70C, "03225075AAAAAAAA" },
    { "STEER_FC",     0x70C, "300001AAAAAAAAAA" },
    { "BRAKE_PRESS",  0x713, "03222B21AAAAAAAA" },
};

static const int s_aim992_fastlog_request_count =
    sizeof(s_aim992_fastlog_requests) / sizeof(s_aim992_fastlog_requests[0]);

static bool cli_send_required_command(const char *cmd) {
    char resp[512] = "";
    if (![g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)] ||
        elm327_is_error_response(resp)) {
        fprintf(stderr, "[ERROR] Command failed: %s", cmd);
        if (resp[0]) fprintf(stderr, " => %s", resp);
        fprintf(stderr, "\n");
        return false;
    }
    return true;
}

static void cli_send_best_effort_command(const char *cmd) {
    char resp[512] = "";
    [g_ble sendCommand:cmd response:resp maxLen:sizeof(resp)];
}

static int cli_p992_fastlog_write_json(const char *out_path,
                                       const char *device_name,
                                       int seconds,
                                       int period_ms) {
    FILE *json_out = stdout;
    if (out_path && *out_path) {
        json_out = fopen(out_path, "w");
        if (!json_out) {
            fprintf(stderr, "[ERROR] Failed to open fastlog output: %s\n", out_path);
            return 1;
        }
    }

    fprintf(json_out, "{\"profile\":\"p992\",\"mode\":\"fastlog_periodic\",\"device\":");
    json_print_string(json_out, device_name);
    fprintf(json_out, ",\"duration_s\":%d,\"period_ms\":%d", seconds, period_ms);
    fprintf(json_out, ",\"schedule\":[");
    for (int i = 0; i < s_aim992_fastlog_request_count; i++) {
        const aim992_fastlog_request_t *req = &s_aim992_fastlog_requests[i];
        if (i > 0) fprintf(json_out, ",");
        fprintf(json_out, "{\"name\":");
        json_print_string(json_out, req->name);
        fprintf(json_out, ",\"tx_id\":\"%03X\",\"payload\":", req->tx_id);
        json_print_string(json_out, req->payload_hex);
        fprintf(json_out, "}");
    }
    fprintf(json_out, "],\"frames\":[\n");

    for (int i = 0; i < g_cli_can_frame_count; i++) {
        if (i > 0) fprintf(json_out, ",\n");
        fprintf(json_out,
                "  {\"timestamp_ms\":%lld,\"id\":\"%03X\",\"is_extended\":%s,\"dlc\":%d,\"data\":\"",
                (long long)g_cli_can_frames[i].timestamp_ms,
                g_cli_can_frames[i].can_id,
                g_cli_can_frames[i].is_extended ? "true" : "false",
                g_cli_can_frames[i].dlc);
        for (int j = 0; j < g_cli_can_frames[i].dlc; j++) {
            if (j > 0) fprintf(json_out, " ");
            fprintf(json_out, "%02X", g_cli_can_frames[i].data[j]);
        }
        fprintf(json_out, "\"}");
    }

    if (g_cli_can_frame_count > 0) fprintf(json_out, "\n");
    fprintf(json_out, "]}\n");

    if (json_out != stdout) {
        fclose(json_out);
        cli_log("Wrote fastlog JSON: %s", out_path);
    }
    return 0;
}

static int cli_p992_fastlog(const char *device,
                            int seconds,
                            int period_ms,
                            const char *out_path) {
    if (seconds <= 0) seconds = 10;
    if (period_ms < 20) period_ms = 20;

    int matchIdx = cli_ble_connect(device);
    char resp[512] = "";

    cli_log("Initializing STN periodic CAN scheduler...");
    const char *init_cmds[] = {
        "ATZ", "ATE0", "ATL0", "ATS1", "ATH1", "ATCAF0",
    };
    for (int i = 0; i < (int)(sizeof(init_cmds) / sizeof(init_cmds[0])); i++) {
        [g_ble sendCommand:init_cmds[i] response:resp maxLen:sizeof(resp)];
        if (strcmp(init_cmds[i], "ATZ") == 0) run_loop_for(0.5);
    }

    char sti_resp[256] = "";
    if ([g_ble sendCommand:"STI" response:sti_resp maxLen:sizeof(sti_resp)] &&
        stn2255_detect(resp, sti_resp, &g_stn_info)) {
        g_stn_detected = true;
        cli_log("STN detected: %s", g_stn_info.device_id);
    }
    if (!g_stn_detected) {
        fprintf(stderr, "[ERROR] fastlog requires STN/OBDLink periodic-message support\n");
        [g_ble disconnect];
        return 1;
    }

    /* Use STN raw CAN 11-bit 500k and schedule full 8-byte CAN frames, matching
       AiM's observed bus traffic. */
    if (!cli_send_required_command("STPPMC") ||
        !cli_send_required_command("STCFCPC") ||
        !cli_send_required_command("STP 31") ||
        !cli_send_required_command("STPO") ||
        !cli_send_required_command("ATCSM0") ||
        !cli_send_required_command("STCMM 1") ||
        !cli_send_required_command("STCSEGR 0") ||
        !cli_send_required_command("STCSEGT 0") ||
        !cli_send_required_command("STFAC")) {
        [g_ble disconnect];
        return 1;
    }

    const uint32_t pass_ids[] = { 0x7DF, 0x7E8, 0x7E9, 0x70C, 0x776, 0x713, 0x77D };
    for (int i = 0; i < (int)(sizeof(pass_ids) / sizeof(pass_ids[0])); i++) {
        char filter_cmd[32];
        snprintf(filter_cmd, sizeof(filter_cmd), "STFAP %03X,7FF", pass_ids[i]);
        if (!cli_send_required_command(filter_cmd)) {
            cli_send_best_effort_command("STPPMC");
            [g_ble disconnect];
            return 1;
        }
    }

    for (int i = 0; i < s_aim992_fastlog_request_count; i++) {
        const aim992_fastlog_request_t *req = &s_aim992_fastlog_requests[i];
        char cmd[96];
        snprintf(cmd, sizeof(cmd), "STPPMA %d, %03X, %s",
                 period_ms, req->tx_id, req->payload_hex);
        if (!cli_send_required_command(cmd)) {
            cli_send_best_effort_command("STPPMC");
            [g_ble disconnect];
            return 1;
        }
        cli_log("Periodic %s: %s", req->name, cmd);
        run_loop_for((double)period_ms / 1000.0 /
                     (double)s_aim992_fastlog_request_count);
    }

    cli_log("Starting filtered raw monitor (STM) for %d seconds...", seconds);
    g_ble->_canLineLen = 0;
    g_ble.canMonitorActive = YES;
    g_cli_can_frame_count = 0;
    memset(g_ble->_responseBuf, 0, sizeof(g_ble->_responseBuf));
    g_ble->_responseLen = 0;
    g_ble->_responseReady = NO;

    [g_ble sendRawString:"STM\r"];

    NSDate *endTime = [NSDate dateWithTimeIntervalSinceNow:seconds];
    while ([[NSDate date] compare:endTime] == NSOrderedAscending &&
           g_ble.isConnected && !g_interrupt) {
        @autoreleasepool {
            [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode
                                     beforeDate:[NSDate dateWithTimeIntervalSinceNow:0.01]];
        }
    }

    g_ble.canMonitorActive = NO;
    [g_ble sendRawString:"\r"];
    run_loop_for(0.2);

    cli_log("Fastlog stopped. Frames: %d", g_cli_can_frame_count);
    cli_send_best_effort_command("STPPMC");
    cli_send_best_effort_command("STCMM 0");

    int ret = cli_p992_fastlog_write_json(out_path,
                                          g_ble.discoveredNames[matchIdx].UTF8String,
                                          seconds,
                                          period_ms);
    [g_ble disconnect];
    g_interrupt = NO;
    return ret;
}

static int cli_p992(int argc, const char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "[ERROR] Usage: p992 <list|read|watch|debug> ...\n");
        return 1;
    }

    const char *subcmd = argv[2];
    if (strcmp(subcmd, "list") == 0) {
        return cli_p992_list();
    }

    if (strcmp(subcmd, "read") == 0) {
        if (argc < 4) {
            fprintf(stderr, "[ERROR] Usage: p992 read <device> [channel...]\n");
            return 1;
        }
        return cli_p992_read(argv[3], argc - 4, &argv[4]);
    }

    if (strcmp(subcmd, "watch") == 0) {
        if (argc < 4) {
            fprintf(stderr,
                    "[ERROR] Usage: p992 watch <device> [--interval-ms N] [--count N] [channel...]\n");
            return 1;
        }

        int interval_ms = 1000;
        int count = 0;
        const char *channel_names[AIM992_MAX_CHANNELS];
        int channel_name_count = 0;

        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--interval-ms") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --interval-ms requires a value\n");
                    return 1;
                }
                interval_ms = atoi(argv[++i]);
            } else if (strncmp(argv[i], "--interval-ms=", 14) == 0) {
                interval_ms = atoi(argv[i] + 14);
            } else if (strcmp(argv[i], "--count") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --count requires a value\n");
                    return 1;
                }
                count = atoi(argv[++i]);
            } else if (strncmp(argv[i], "--count=", 8) == 0) {
                count = atoi(argv[i] + 8);
            } else if (channel_name_count < AIM992_MAX_CHANNELS) {
                channel_names[channel_name_count++] = argv[i];
            } else {
                fprintf(stderr, "[ERROR] Too many channel names\n");
                return 1;
            }
        }

        if (interval_ms < 100) interval_ms = 100;
        return cli_p992_watch(argv[3], interval_ms, count,
                              channel_name_count, channel_names);
    }

    if (strcmp(subcmd, "fastlog") == 0) {
        if (argc < 4) {
            fprintf(stderr, "[ERROR] Usage: p992 fastlog <device> [seconds] [--period-ms N] [--out FILE]\n");
            return 1;
        }

        int seconds = 10;
        int period_ms = 166;
        const char *out_path = NULL;
        int arg_start = 4;

        if (argc >= 5) {
            char *endp = NULL;
            long val = strtol(argv[4], &endp, 10);
            if (endp && *endp == '\0' && val > 0) {
                seconds = (int)val;
                arg_start = 5;
            }
        }

        for (int i = arg_start; i < argc; i++) {
            if (strcmp(argv[i], "--period-ms") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --period-ms requires a value\n");
                    return 1;
                }
                period_ms = atoi(argv[++i]);
            } else if (strncmp(argv[i], "--period-ms=", 12) == 0) {
                period_ms = atoi(argv[i] + 12);
            } else if (strcmp(argv[i], "--out") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --out requires a file path\n");
                    return 1;
                }
                out_path = argv[++i];
            } else if (strncmp(argv[i], "--out=", 6) == 0) {
                out_path = argv[i] + 6;
            } else {
                fprintf(stderr, "[ERROR] Unexpected fastlog argument: %s\n", argv[i]);
                return 1;
            }
        }

        return cli_p992_fastlog(argv[3], seconds, period_ms, out_path);
    }

    if (strcmp(subcmd, "debug") == 0) {
        if (argc < 4) {
            fprintf(stderr, "[ERROR] Usage: p992 debug <device>\n");
            return 1;
        }
        return cli_p992_debug(argv[3]);
    }

    if (strcmp(subcmd, "bin-probe") == 0) {
        if (argc < 4) {
            fprintf(stderr, "[ERROR] Usage: p992 bin-probe <device>\n");
            return 1;
        }
        return cli_p992_bin_probe(argv[3]);
    }

    if (strcmp(subcmd, "scan-did") == 0) {
        uint16_t did = 0x5101;
        int session = -1;
        bool did_set = false;
        if (argc < 4) {
            fprintf(stderr, "[ERROR] Usage: p992 scan-did <device> [DID_hex] [--session XX]\n");
            return 1;
        }

        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--session") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --session requires a hex byte\n");
                    return 1;
                }
                unsigned int val = 0;
                if (sscanf(argv[++i], "%x", &val) != 1 || val > 0xFF) {
                    fprintf(stderr, "[ERROR] Invalid session hex: %s\n", argv[i]);
                    return 1;
                }
                session = (int)val;
            } else if (strncmp(argv[i], "--session=", 10) == 0) {
                unsigned int val = 0;
                if (sscanf(argv[i] + 10, "%x", &val) != 1 || val > 0xFF) {
                    fprintf(stderr, "[ERROR] Invalid session hex: %s\n", argv[i] + 10);
                    return 1;
                }
                session = (int)val;
            } else if (!did_set) {
                unsigned int val = 0;
                if (sscanf(argv[i], "%x", &val) != 1 || val > 0xFFFF) {
                    fprintf(stderr, "[ERROR] Invalid DID hex: %s\n", argv[i]);
                    return 1;
                }
                did = (uint16_t)val;
                did_set = true;
            } else {
                fprintf(stderr, "[ERROR] Unexpected scan-did argument: %s\n", argv[i]);
                return 1;
            }
        }
        return cli_p992_scan_did(argv[3], did, session);
    }

    if (strcmp(subcmd, "scan-did-range") == 0) {
        uint16_t start_did = 0x50F0;
        uint16_t end_did = 0x5110;
        if (argc < 4) {
            fprintf(stderr, "[ERROR] Usage: p992 scan-did-range <device> [START_DID_hex END_DID_hex]\n");
            return 1;
        }
        if (argc >= 6) {
            unsigned int start_val = 0;
            unsigned int end_val = 0;
            if (sscanf(argv[4], "%x", &start_val) != 1 || start_val > 0xFFFF ||
                sscanf(argv[5], "%x", &end_val) != 1 || end_val > 0xFFFF) {
                fprintf(stderr, "[ERROR] Invalid DID range: %s %s\n", argv[4], argv[5]);
                return 1;
            }
            start_did = (uint16_t)start_val;
            end_did = (uint16_t)end_val;
        } else if (argc != 4) {
            fprintf(stderr, "[ERROR] Usage: p992 scan-did-range <device> [START_DID_hex END_DID_hex]\n");
            return 1;
        }
        return cli_p992_scan_did_range(argv[3], start_did, end_did);
    }

    if (strcmp(subcmd, "uds-read") == 0) {
        uint16_t did = 0;
        uint32_t tx_id = 0x7E0;
        int rx_id = -1;
        int session = -1;
        const char *pre_request_hex = NULL;
        bool did_set = false;

        if (argc < 5) {
            fprintf(stderr, "[ERROR] Usage: p992 uds-read <device> <DID_hex> [--tx-id XXX] [--rx-id XXX] [--session XX] [--pre HEX]\n");
            return 1;
        }

        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--tx-id") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --tx-id requires a hex CAN ID\n");
                    return 1;
                }
                unsigned int val = 0;
                if (sscanf(argv[++i], "%x", &val) != 1 || val > 0x7FF) {
                    fprintf(stderr, "[ERROR] Invalid tx CAN ID: %s\n", argv[i]);
                    return 1;
                }
                tx_id = val;
            } else if (strncmp(argv[i], "--tx-id=", 8) == 0) {
                unsigned int val = 0;
                if (sscanf(argv[i] + 8, "%x", &val) != 1 || val > 0x7FF) {
                    fprintf(stderr, "[ERROR] Invalid tx CAN ID: %s\n", argv[i] + 8);
                    return 1;
                }
                tx_id = val;
            } else if (strcmp(argv[i], "--rx-id") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --rx-id requires a hex CAN ID\n");
                    return 1;
                }
                unsigned int val = 0;
                if (sscanf(argv[++i], "%x", &val) != 1 || val > 0x7FF) {
                    fprintf(stderr, "[ERROR] Invalid rx CAN ID: %s\n", argv[i]);
                    return 1;
                }
                rx_id = (int)val;
            } else if (strncmp(argv[i], "--rx-id=", 8) == 0) {
                unsigned int val = 0;
                if (sscanf(argv[i] + 8, "%x", &val) != 1 || val > 0x7FF) {
                    fprintf(stderr, "[ERROR] Invalid rx CAN ID: %s\n", argv[i] + 8);
                    return 1;
                }
                rx_id = (int)val;
            } else if (strcmp(argv[i], "--session") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --session requires a hex byte\n");
                    return 1;
                }
                unsigned int val = 0;
                if (sscanf(argv[++i], "%x", &val) != 1 || val > 0xFF) {
                    fprintf(stderr, "[ERROR] Invalid session hex: %s\n", argv[i]);
                    return 1;
                }
                session = (int)val;
            } else if (strncmp(argv[i], "--session=", 10) == 0) {
                unsigned int val = 0;
                if (sscanf(argv[i] + 10, "%x", &val) != 1 || val > 0xFF) {
                    fprintf(stderr, "[ERROR] Invalid session hex: %s\n", argv[i] + 10);
                    return 1;
                }
                session = (int)val;
            } else if (strcmp(argv[i], "--pre") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --pre requires a hex request\n");
                    return 1;
                }
                pre_request_hex = argv[++i];
            } else if (strncmp(argv[i], "--pre=", 6) == 0) {
                pre_request_hex = argv[i] + 6;
            } else if (!did_set) {
                unsigned int val = 0;
                if (sscanf(argv[i], "%x", &val) != 1 || val > 0xFFFF) {
                    fprintf(stderr, "[ERROR] Invalid DID hex: %s\n", argv[i]);
                    return 1;
                }
                did = (uint16_t)val;
                did_set = true;
            } else {
                fprintf(stderr, "[ERROR] Unexpected uds-read argument: %s\n", argv[i]);
                return 1;
            }
        }

        if (!did_set) {
            fprintf(stderr, "[ERROR] uds-read requires a DID hex value\n");
            return 1;
        }
        return cli_p992_uds_read(argv[3], did, tx_id, rx_id, session, pre_request_hex);
    }

    if (strcmp(subcmd, "uds-raw") == 0) {
        const char *request_hex = NULL;
        uint32_t tx_id = 0x7E0;
        int rx_id = -1;
        int session = -1;

        if (argc < 5) {
            fprintf(stderr, "[ERROR] Usage: p992 uds-raw <device> <REQ_hex> [--tx-id XXX] [--rx-id XXX] [--session XX]\n");
            return 1;
        }

        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--tx-id") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --tx-id requires a hex CAN ID\n");
                    return 1;
                }
                unsigned int val = 0;
                if (sscanf(argv[++i], "%x", &val) != 1 || val > 0x7FF) {
                    fprintf(stderr, "[ERROR] Invalid tx CAN ID: %s\n", argv[i]);
                    return 1;
                }
                tx_id = val;
            } else if (strncmp(argv[i], "--tx-id=", 8) == 0) {
                unsigned int val = 0;
                if (sscanf(argv[i] + 8, "%x", &val) != 1 || val > 0x7FF) {
                    fprintf(stderr, "[ERROR] Invalid tx CAN ID: %s\n", argv[i] + 8);
                    return 1;
                }
                tx_id = val;
            } else if (strcmp(argv[i], "--rx-id") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --rx-id requires a hex CAN ID\n");
                    return 1;
                }
                unsigned int val = 0;
                if (sscanf(argv[++i], "%x", &val) != 1 || val > 0x7FF) {
                    fprintf(stderr, "[ERROR] Invalid rx CAN ID: %s\n", argv[i]);
                    return 1;
                }
                rx_id = (int)val;
            } else if (strncmp(argv[i], "--rx-id=", 8) == 0) {
                unsigned int val = 0;
                if (sscanf(argv[i] + 8, "%x", &val) != 1 || val > 0x7FF) {
                    fprintf(stderr, "[ERROR] Invalid rx CAN ID: %s\n", argv[i] + 8);
                    return 1;
                }
                rx_id = (int)val;
            } else if (strcmp(argv[i], "--session") == 0) {
                if (i + 1 >= argc) {
                    fprintf(stderr, "[ERROR] --session requires a hex byte\n");
                    return 1;
                }
                unsigned int val = 0;
                if (sscanf(argv[++i], "%x", &val) != 1 || val > 0xFF) {
                    fprintf(stderr, "[ERROR] Invalid session hex: %s\n", argv[i]);
                    return 1;
                }
                session = (int)val;
            } else if (strncmp(argv[i], "--session=", 10) == 0) {
                unsigned int val = 0;
                if (sscanf(argv[i] + 10, "%x", &val) != 1 || val > 0xFF) {
                    fprintf(stderr, "[ERROR] Invalid session hex: %s\n", argv[i] + 10);
                    return 1;
                }
                session = (int)val;
            } else if (!request_hex) {
                request_hex = argv[i];
            } else {
                fprintf(stderr, "[ERROR] Unexpected uds-raw argument: %s\n", argv[i]);
                return 1;
            }
        }

        if (!request_hex) {
            fprintf(stderr, "[ERROR] uds-raw requires a request hex value\n");
            return 1;
        }
        return cli_p992_uds_raw(argv[3], request_hex, tx_id, rx_id, session);
    }

    if (strcmp(subcmd, "help") == 0 || strcmp(subcmd, "--help") == 0) {
        fprintf(stderr, "Usage:\n");
        fprintf(stderr, "  macos_obd_reader p992 list\n");
        fprintf(stderr, "  macos_obd_reader p992 read <device> [channel...]\n");
        fprintf(stderr, "  macos_obd_reader p992 watch <device> [--interval-ms N] [--count N] [channel...]\n");
        fprintf(stderr, "  macos_obd_reader p992 fastlog <device> [secs] [--period-ms N] [--out FILE]\n");
        fprintf(stderr, "  macos_obd_reader p992 debug <device>\n");
        fprintf(stderr, "  macos_obd_reader p992 bin-probe <device>\n");
        fprintf(stderr, "  macos_obd_reader p992 scan-did <device> [DID_hex] [--session XX]\n");
        fprintf(stderr, "  macos_obd_reader p992 scan-did-range <device> [START END]\n");
        fprintf(stderr, "  macos_obd_reader p992 uds-read <device> <DID> [...]\n");
        fprintf(stderr, "  macos_obd_reader p992 uds-raw <device> <REQ> [...]\n");
        return 0;
    }

    fprintf(stderr, "[ERROR] Unknown p992 command: %s\n", subcmd);
    return 1;
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

static const char *disc_phase_slug(int phase_idx) {
    static const char *kPhaseSlugs[DISC_NUM_PHASES] = {
        "baseline", "steering", "throttle", "brake", "gear", "wheel_speed"
    };
    if (phase_idx < 0 || phase_idx >= DISC_NUM_PHASES) return "phase";
    return kPhaseSlugs[phase_idx];
}

static const char *disc_phase_display_name(int phase_idx) {
    static const char *kPhaseNames[DISC_NUM_PHASES] = {
        "Baseline", "Steering", "Throttle", "Brake", "Gear", "Wheel Speed"
    };
    if (phase_idx < 0 || phase_idx >= DISC_NUM_PHASES) return "Phase";
    return kPhaseNames[phase_idx];
}

static void disc_join_path(char *buf, size_t len,
                           const char *dir, const char *leaf) {
    if (!buf || len == 0) return;
    if (!dir || !dir[0]) {
        snprintf(buf, len, "%s", leaf ? leaf : "");
        return;
    }

    size_t dir_len = strlen(dir);
    if (dir_len > 0 && dir[dir_len - 1] == '/') {
        snprintf(buf, len, "%s%s", dir, leaf);
    } else {
        snprintf(buf, len, "%s/%s", dir, leaf);
    }
}

static void disc_make_session_dir_path(char *buf, size_t len,
                                       const char *dump_dir_override) {
    if (dump_dir_override && dump_dir_override[0]) {
        snprintf(buf, len, "%s", dump_dir_override);
        return;
    }

    time_t now = time(NULL);
    struct tm tm_now;
    localtime_r(&now, &tm_now);
    char stamp[32];
    strftime(stamp, sizeof(stamp), "%Y%m%d_%H%M%S", &tm_now);
    snprintf(buf, len, "captures/%s", stamp);
}

static bool disc_ensure_dir(const char *path) {
    if (!path || !path[0]) return false;

    NSFileManager *fm = [NSFileManager defaultManager];
    NSString *ns_path = [NSString stringWithUTF8String:path];
    NSError *error = nil;
    BOOL ok = [fm createDirectoryAtPath:ns_path
            withIntermediateDirectories:YES
                             attributes:nil
                                  error:&error];
    if (!ok) {
        fprintf(stderr, "[WARN] Failed to create dump dir %s: %s\n",
                path, error.localizedDescription.UTF8String);
    }
    return ok == YES;
}

static void disc_make_draft_dbc_path(char *buf, size_t len, const char *dump_dir) {
    if (dump_dir && dump_dir[0]) {
        disc_join_path(buf, len, dump_dir, "ptcan_discover_draft.dbc");
        return;
    }

    time_t now = time(NULL);
    struct tm tm_now;
    localtime_r(&now, &tm_now);
    strftime(buf, len, "ptcan_discover_draft_%Y%m%d_%H%M%S.dbc", &tm_now);
}

static void disc_make_result_json_path(char *buf, size_t len, const char *dump_dir) {
    if (dump_dir && dump_dir[0]) {
        disc_join_path(buf, len, dump_dir, "result.json");
    } else {
        snprintf(buf, len, "result.json");
    }
}

static void disc_make_manifest_path(char *buf, size_t len, const char *dump_dir) {
    if (dump_dir && dump_dir[0]) {
        disc_join_path(buf, len, dump_dir, "manifest.json");
    } else {
        snprintf(buf, len, "manifest.json");
    }
}

static void disc_make_phase_dump_filename(char *buf, size_t len, int phase_idx) {
    snprintf(buf, len, "%02d_%s.frames.tsv", phase_idx, disc_phase_slug(phase_idx));
}

static void disc_make_phase_dump_path(char *buf, size_t len,
                                      const char *dump_dir, int phase_idx) {
    char leaf[64];
    disc_make_phase_dump_filename(leaf, sizeof(leaf), phase_idx);
    disc_join_path(buf, len, dump_dir, leaf);
}

static bool disc_write_phase_dump(const char *path, const disc_raw_phase_t *raw_phase) {
    if (!path || !raw_phase) return false;

    FILE *f = fopen(path, "w");
    if (!f) return false;

    fprintf(f, "timestamp_ms\tcan_id\tis_extended\tdlc\tdata_hex\n");
    for (int i = 0; i < raw_phase->frame_count; i++) {
        const can_frame_t *frame = &raw_phase->frames[i];
        fprintf(f, "%lld\t0x%X\t%d\t%u\t",
                (long long)frame->timestamp_ms,
                frame->can_id,
                frame->is_extended ? 1 : 0,
                frame->dlc);
        for (int b = 0; b < frame->dlc; b++) {
            fprintf(f, "%02X", frame->data[b]);
        }
        fputc('\n', f);
    }

    fclose(f);
    return true;
}

static void disc_write_result_json_object(FILE *f,
                                          bool have_bus_mode,
                                          disc_bus_mode_t bus_mode,
                                          const char *dump_dir,
                                          const char *dbc_path,
                                          const disc_result_t *result) {
    bool first = true;

    fprintf(f, "{\n");

    if (have_bus_mode) {
        fprintf(f, "  \"bus_mode\":");
        json_print_string(f, disc_bus_mode_name(bus_mode));
        first = false;
    }

    if (dump_dir && dump_dir[0]) {
        fprintf(f, "%s  \"dump_dir\":", first ? "" : ",\n");
        json_print_string(f, dump_dir);
        first = false;
    }

    if (dbc_path && dbc_path[0]) {
        fprintf(f, "%s  \"dbc_path\":", first ? "" : ",\n");
        json_print_string(f, dbc_path);
        first = false;
    }

    for (int s = 0; result && s < DISC_SIG_COUNT; s++) {
        if (!result->found[s]) continue;

        const disc_candidate_t *c = &result->signals[s];
        fprintf(f, "%s  ", first ? "" : ",\n");
        json_print_string(f, disc_signal_name((disc_signal_t)s));
        fprintf(f, ":{\"can_id\":\"0x%03X\",\"dlc\":%d,\"hz\":%.1f",
                c->can_id, c->dlc, c->hz);
        if (c->byte_idx >= 0) fprintf(f, ",\"byte\":%d", c->byte_idx);
        if (c->byte2_idx >= 0) fprintf(f, ",\"byte2\":%d", c->byte2_idx);
        fprintf(f, ",\"score\":%.1f,\"confidence\":\"%s\"",
                c->score, disc_confidence_name(c->confidence));
        if (c->byte2_idx >= 0 && c->endianness != DISC_ENDIAN_UNKNOWN) {
            fprintf(f, ",\"endian\":\"%s\"", disc_endian_name(c->endianness));
        }
        if (c->signedness != DISC_SIGN_UNKNOWN) {
            fprintf(f, ",\"signed\":%s",
                    c->signedness == DISC_SIGN_SIGNED ? "true" : "false");
            fprintf(f, ",\"raw_min\":%d,\"raw_max\":%d", c->raw_min, c->raw_max);
        }
        fprintf(f, "}");
        first = false;
    }

    fprintf(f, "\n}\n");
}

static bool disc_write_result_json(const char *path,
                                   bool have_bus_mode,
                                   disc_bus_mode_t bus_mode,
                                   const char *dump_dir,
                                   const char *dbc_path,
                                   const disc_result_t *result) {
    if (!path) return false;

    FILE *f = fopen(path, "w");
    if (!f) return false;
    disc_write_result_json_object(f, have_bus_mode, bus_mode, dump_dir, dbc_path, result);
    fclose(f);
    return true;
}

static bool disc_write_manifest(const char *path,
                                const char *status,
                                const char *device_prefix,
                                const char *connected_name,
                                const char *monitor_cmd,
                                bool have_bus_mode,
                                disc_bus_mode_t bus_mode,
                                const char *dbc_path,
                                const char *result_path,
                                const disc_phase_dump_meta_t metas[DISC_NUM_PHASES]) {
    if (!path || !status || !metas) return false;

    FILE *f = fopen(path, "w");
    if (!f) return false;

    fprintf(f, "{\n");
    fprintf(f, "  \"format\":");
    json_print_string(f, "can_discover_dump/v1");
    fprintf(f, ",\n  \"status\":");
    json_print_string(f, status);
    fprintf(f, ",\n  \"device_prefix\":");
    if (device_prefix && device_prefix[0]) json_print_string(f, device_prefix);
    else fputs("null", f);
    fprintf(f, ",\n  \"connected_device\":");
    if (connected_name && connected_name[0]) json_print_string(f, connected_name);
    else fputs("null", f);
    fprintf(f, ",\n  \"monitor_cmd\":");
    json_print_string(f, monitor_cmd ? monitor_cmd : "");
    fprintf(f, ",\n  \"bus_mode\":");
    if (have_bus_mode) json_print_string(f, disc_bus_mode_name(bus_mode));
    else fputs("null", f);
    fprintf(f, ",\n  \"dbc_path\":");
    if (dbc_path && dbc_path[0]) json_print_string(f, dbc_path);
    else fputs("null", f);
    fprintf(f, ",\n  \"result_path\":");
    if (result_path && result_path[0]) json_print_string(f, result_path);
    else fputs("null", f);
    fprintf(f, ",\n  \"phases\":[\n");

    for (int i = 0; i < DISC_NUM_PHASES; i++) {
        const disc_phase_dump_meta_t *meta = &metas[i];
        if (i > 0) fprintf(f, ",\n");
        fprintf(f, "    {\"index\":%d,\"name\":", i);
        json_print_string(f, disc_phase_display_name(i));
        fprintf(f, ",\"slug\":");
        json_print_string(f, disc_phase_slug(i));
        fprintf(f, ",\"planned_duration_s\":%d", meta->planned_duration_s);
        fprintf(f, ",\"skipped\":%s", meta->skipped ? "true" : "false");
        fprintf(f, ",\"captured\":%s", meta->captured ? "true" : "false");
        fprintf(f, ",\"actual_duration_s\":%.3f", meta->actual_duration_s);
        fprintf(f, ",\"frame_count\":%d", meta->total_frames);
        fprintf(f, ",\"id_count\":%d", meta->id_count);
        fprintf(f, ",\"dump_file\":");
        if (meta->dump_file[0]) json_print_string(f, meta->dump_file);
        else fputs("null", f);
        fprintf(f, "}");
    }

    fprintf(f, "\n  ]\n}\n");
    fclose(f);
    return true;
}

/**
 * BLE scan + connect (shared by all CLI subcommands).
 * Scans, matches device by name prefix, connects, waits for service discovery.
 * Returns matched device index, or exits with code 1 on failure.
 */
static int cli_ble_connect(const char *device_prefix) {
    /* BLE may be unavailable while Classic RFCOMM still works. Scan anyway so
       Classic adapters like vLinker MS can be selected. */
    cli_log("Waiting for Bluetooth...");
    if (![g_ble isBleReady]) {
        wait_for(&(g_ble->_bleReady), 10.0);
    }
    if (![g_ble isBleReady]) {
        fprintf(stderr, "[WARN] BLE not available; scanning paired/Classic devices only\n");
    }

    /* Scan */
    cli_log("Scanning for devices...");
    [g_ble startScan];
    run_loop_for(8.0);
    [g_ble stopScan];

    if (g_ble.discoveredNames.count == 0) {
        fprintf(stderr, "[WARN] No OBD devices found on first pass; retrying Classic scan\n");
        [g_ble startScan];
        run_loop_for(8.0);
        [g_ble stopScan];
    }

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

    OBDTransportType transport = (OBDTransportType)[g_ble.discoveredTransports[matchIdx] intValue];
    if (transport == OBD_TRANSPORT_BLE && ![g_ble isBleReady]) {
        fprintf(stderr, "[ERROR] Matched device requires BLE, but BLE is not available\n");
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
 * Emit an operator cue after Bluetooth/CAN setup is complete and immediately
 * before the monitor command is sent. This keeps live test timing aligned with
 * the actual capture window.
 */
static void cli_monitor_cue_before_start(void) {
    const char *label = getenv("CAN_TOOLS_MONITOR_LABEL");
    if (!label || !*label) return;

    int countdown = 0;
    const char *countdown_env = getenv("CAN_TOOLS_MONITOR_COUNTDOWN");
    if (countdown_env && *countdown_env) {
        countdown = atoi(countdown_env);
        if (countdown < 0) countdown = 0;
        if (countdown > 30) countdown = 30;
    }

    fprintf(stderr, "[CUE] %s: ready\n", label);
    fflush(stderr);
    for (int n = countdown; n > 0; n--) {
        fprintf(stderr, "[CUE] %s: start in %d\n", label, n);
        fflush(stderr);
        run_loop_for(1.0);
    }

    fprintf(stderr, "\a[CUE] %s: START NOW\n", label);
    fflush(stderr);
}

static void cli_monitor_cue_after_stop(void) {
    const char *label = getenv("CAN_TOOLS_MONITOR_LABEL");
    if (!label || !*label) return;
    fprintf(stderr, "\a[CUE] %s: STOP\n", label);
    fflush(stderr);
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

    cli_monitor_cue_before_start();

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
    cli_monitor_cue_after_stop();

    /* JSON output */
    FILE *json_out = stdout;
    const char *monitor_out_path = g_cli_monitor_out_path;
    if (!monitor_out_path || !*monitor_out_path) {
        monitor_out_path = getenv("CAN_TOOLS_MONITOR_OUT");
    }
    if (monitor_out_path && *monitor_out_path) {
        json_out = fopen(monitor_out_path, "w");
        if (!json_out) {
            fprintf(stderr, "[ERROR] Failed to open monitor output: %s\n", monitor_out_path);
            [g_ble disconnect];
            g_interrupt = NO;
            return 1;
        }
    }

    fprintf(json_out, "{\"device\":");
    json_print_string(json_out, g_ble.discoveredNames[matchIdx].UTF8String);
    fprintf(json_out, ",\"mode\":\"silent\",\"duration_s\":%d", seconds);
    if (filter_count > 0) {
        fprintf(json_out, ",\"filters\":[");
        for (int i = 0; i < filter_count; i++) {
            if (i > 0) fprintf(json_out, ",");
            fprintf(json_out, "\"%03X\"", filter_ids[i]);
        }
        fprintf(json_out, "]");
    }
    fprintf(json_out, ",\"frames\":[\n");

    for (int i = 0; i < g_cli_can_frame_count; i++) {
        if (i > 0) fprintf(json_out, ",\n");
        fprintf(json_out, "  {\"timestamp_ms\":%lld,\"id\":\"%03X\",\"is_extended\":%s,\"dlc\":%d,\"data\":\"",
                (long long)g_cli_can_frames[i].timestamp_ms,
                g_cli_can_frames[i].can_id,
                g_cli_can_frames[i].is_extended ? "true" : "false",
                g_cli_can_frames[i].dlc);
        for (int j = 0; j < g_cli_can_frames[i].dlc; j++) {
            if (j > 0) fprintf(json_out, " ");
            fprintf(json_out, "%02X", g_cli_can_frames[i].data[j]);
        }
        fprintf(json_out, "\"}");
    }

    if (g_cli_can_frame_count > 0) fprintf(json_out, "\n");
    fprintf(json_out, "]}\n");

    if (json_out != stdout) {
        fclose(json_out);
        cli_log("Wrote monitor JSON: %s", monitor_out_path);
    }

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

/** Read a single char from stdin while pumping NSRunLoop for BLE.
 *  Drains any remaining characters on the line (up to and including '\n')
 *  so the next call doesn't accidentally consume a stale newline. */
static int disc_read_char(void) {
    int ch = 0;
    while (ch == 0 && g_ble.isConnected && g_running) {
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
    /* Drain rest of the line to prevent stale newline on next call */
    if (ch != '\n' && ch != 0) {
        int c;
        while ((c = fgetc(stdin)) != EOF && c != '\n')
            ;
    }
    return ch;
}

/** Capture one phase. Returns false if connection lost. */
static bool disc_capture_phase(disc_phase_t *phase, disc_raw_phase_t *raw_phase,
                                 int duration_s, const char *monitor_cmd) {
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
    if (raw_phase &&
        !disc_raw_phase_set(raw_phase, g_cli_can_frames, g_cli_can_frame_count)) {
        fprintf(stderr, "\n[WARN] Failed to store raw frames for DBC draft output\n");
    }

    /* Reset interrupt so next capture isn't immediately skipped */
    g_interrupt = NO;

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

static int cli_discover(const char *device, const char *dump_dir_override) {
    int matchIdx = cli_ptcan_connect(device, NULL, 0);
    const char *connected_name = NULL;
    if (matchIdx >= 0 && matchIdx < (int)g_ble.discoveredNames.count) {
        connected_name = g_ble.discoveredNames[matchIdx].UTF8String;
    }

    const char *monitor_cmd = g_stn_detected ? "STMA" : "ATMA";
    char dump_dir[DISC_PATH_MAX] = {0};
    char dbc_path[DISC_PATH_MAX] = {0};
    char result_path[DISC_PATH_MAX] = {0};
    char manifest_path[DISC_PATH_MAX] = {0};
    bool dump_enabled = false;
    bool have_bus_mode = false;
    disc_bus_mode_t bus_mode = DISC_BUS_DIRECT;

    disc_phase_t phases[DISC_NUM_PHASES];
    disc_raw_phase_t raw_phases[DISC_NUM_PHASES];
    disc_phase_dump_meta_t phase_meta[DISC_NUM_PHASES];
    for (int i = 0; i < DISC_NUM_PHASES; i++) {
        disc_phase_init(&phases[i]);
        disc_raw_phase_init(&raw_phases[i]);
        memset(&phase_meta[i], 0, sizeof(phase_meta[i]));
        phase_meta[i].planned_duration_s = s_disc_phases[i].duration_s;
    }

    disc_make_session_dir_path(dump_dir, sizeof(dump_dir), dump_dir_override);
    if (disc_ensure_dir(dump_dir)) {
        dump_enabled = true;
        cli_log("Capture dump dir: %s", dump_dir);
        disc_make_manifest_path(manifest_path, sizeof(manifest_path), dump_dir);
        disc_make_result_json_path(result_path, sizeof(result_path), dump_dir);
    }

    /* Keep exclusions empty so multiple signals can share one CAN message. */
    uint32_t excl[DISC_SIG_COUNT] = {0};
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

    if (!g_running) goto interrupted;
    if (!g_ble.isConnected) goto disconnected;

    fprintf(stderr, "  Capturing %d seconds...", s_disc_phases[0].duration_s);
    fflush(stderr);
    if (!disc_capture_phase(&phases[DISC_PHASE_BASELINE],
                            &raw_phases[DISC_PHASE_BASELINE],
                            s_disc_phases[0].duration_s, monitor_cmd))
        goto disconnected;
    fprintf(stderr, " %d frames, %d CAN IDs\n",
            phases[DISC_PHASE_BASELINE].total_frames,
            phases[DISC_PHASE_BASELINE].id_count);

    /* Detect bus mode (D-CAN gateway vs direct PT-CAN) */
    bus_mode = disc_detect_bus_mode(&phases[DISC_PHASE_BASELINE]);
    have_bus_mode = true;
    cli_log("Bus mode: %s (%s)",
            bus_mode == DISC_BUS_GATEWAY ? "GATEWAY" : "DIRECT",
            bus_mode == DISC_BUS_GATEWAY ? "D-CAN" : "PT-CAN");

    result.bus_mode = bus_mode;
    phase_meta[DISC_PHASE_BASELINE].captured = true;
    phase_meta[DISC_PHASE_BASELINE].actual_duration_s = phases[DISC_PHASE_BASELINE].duration_s;
    phase_meta[DISC_PHASE_BASELINE].total_frames = phases[DISC_PHASE_BASELINE].total_frames;
    phase_meta[DISC_PHASE_BASELINE].id_count = phases[DISC_PHASE_BASELINE].id_count;
    if (dump_enabled) {
        char phase_path[DISC_PATH_MAX];
        disc_make_phase_dump_path(phase_path, sizeof(phase_path),
                                  dump_dir, DISC_PHASE_BASELINE);
        if (disc_write_phase_dump(phase_path, &raw_phases[DISC_PHASE_BASELINE])) {
            disc_make_phase_dump_filename(phase_meta[DISC_PHASE_BASELINE].dump_file,
                                          sizeof(phase_meta[DISC_PHASE_BASELINE].dump_file),
                                          DISC_PHASE_BASELINE);
        } else {
            fprintf(stderr, "[WARN] Failed to write phase dump: %s\n", phase_path);
        }
    }

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
        if (!g_running) goto interrupted;
        if (!g_ble.isConnected) goto disconnected;
        if (ch == 's' || ch == 'S') {
            fprintf(stderr, "  Skipped.\n");
            phase_meta[p].skipped = true;
            continue;
        }

        /* Capture */
        disc_phase_t *active = &phases[p];

        fprintf(stderr, "  Capturing %d seconds...", s_disc_phases[p].duration_s);
        fflush(stderr);
        if (!disc_capture_phase(active, &raw_phases[p],
                                s_disc_phases[p].duration_s, monitor_cmd))
            goto disconnected;
        fprintf(stderr, " %d frames, %d CAN IDs\n",
                active->total_frames, active->id_count);
        phase_meta[p].captured = true;
        phase_meta[p].actual_duration_s = active->duration_s;
        phase_meta[p].total_frames = active->total_frames;
        phase_meta[p].id_count = active->id_count;
        if (dump_enabled) {
            char phase_path[DISC_PATH_MAX];
            disc_make_phase_dump_path(phase_path, sizeof(phase_path), dump_dir, p);
            if (disc_write_phase_dump(phase_path, &raw_phases[p])) {
                disc_make_phase_dump_filename(phase_meta[p].dump_file,
                                              sizeof(phase_meta[p].dump_file), p);
            } else {
                fprintf(stderr, "[WARN] Failed to write phase dump: %s\n", phase_path);
            }
        }

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

        bool found = disc_classify_with_raw(&phases[DISC_PHASE_BASELINE],
                                            &raw_phases[DISC_PHASE_BASELINE],
                                            active, &raw_phases[p], sig,
                                            excl, excl_count, &cand,
                                            &rpm_cand, &rpm_det);

        /* Characterize detected signals */
        if (found) disc_characterize(active, &cand);
        if (rpm_det) disc_characterize(active, &rpm_cand);

        if (sig == DISC_SIG_THROTTLE) {
            /* Show RPM result first */
            if (rpm_det) {
                disc_print_candidate("rpm", &rpm_cand);
                fprintf(stderr, "  Include rpm? [y/n] > ");
                fflush(stderr);
                int c2 = disc_read_char();
                if (!g_running) goto interrupted;
                if (!g_ble.isConnected) goto disconnected;
                if (c2 == 'y' || c2 == 'Y' || c2 == '\n') {
                    result.signals[DISC_SIG_RPM] = rpm_cand;
                    result.found[DISC_SIG_RPM] = true;
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
                if (!g_running) goto interrupted;
                if (!g_ble.isConnected) goto disconnected;
                if (c2 == 'y' || c2 == 'Y' || c2 == '\n') {
                    result.signals[DISC_SIG_THROTTLE] = cand;
                    result.found[DISC_SIG_THROTTLE] = true;
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
                if (!g_running) goto interrupted;
                if (!g_ble.isConnected) goto disconnected;
                if (c2 == 'y' || c2 == 'Y' || c2 == '\n') {
                    result.signals[sig] = cand;
                    result.found[sig] = true;
                    fprintf(stderr, "  -> %s included.\n", sname);
                } else {
                    fprintf(stderr, "  -> %s excluded.\n", sname);
                }
            } else {
                fprintf(stderr, "    %-12s not detected\n", sname);
            }
        }
    }

    disc_make_draft_dbc_path(dbc_path, sizeof(dbc_path),
                             dump_enabled ? dump_dir : NULL);
    bool dbc_written = disc_write_draft_dbc(dbc_path, phases, raw_phases, &result);
    if (dbc_written) {
        cli_log("Draft DBC written: %s", dbc_path);
    } else {
        fprintf(stderr, "[WARN] Failed to write draft DBC: %s\n", dbc_path);
    }

    if (dump_enabled) {
        if (!disc_write_result_json(result_path, have_bus_mode, bus_mode,
                                    dump_dir, dbc_written ? dbc_path : NULL, &result)) {
            fprintf(stderr, "[WARN] Failed to write result JSON: %s\n", result_path);
            result_path[0] = '\0';
        }
        if (!disc_write_manifest(manifest_path, "completed", device,
                                 connected_name, monitor_cmd,
                                 have_bus_mode, bus_mode,
                                 dbc_written ? dbc_path : NULL,
                                 result_path[0] ? result_path : NULL,
                                 phase_meta)) {
            fprintf(stderr, "[WARN] Failed to write manifest: %s\n", manifest_path);
        }
    }

    /* JSON output (stdout) — only confirmed signals */
    disc_write_result_json_object(stdout, have_bus_mode, bus_mode,
                                  dump_enabled ? dump_dir : NULL,
                                  dbc_written ? dbc_path : NULL, &result);

    /* Summary to stderr */
    fprintf(stderr, "\n=== Final Results ===\n");
    for (int s = 0; s < DISC_SIG_COUNT; s++) {
        if (result.found[s]) {
            disc_print_candidate(disc_signal_name((disc_signal_t)s),
                                  &result.signals[s]);
        }
    }

    disc_free_raw_phases(raw_phases);
    [g_ble disconnect];
    g_interrupt = NO;
    return 0;

interrupted:
    fprintf(stderr, "\n[INFO] Interrupted by user\n");
    if (dump_enabled) {
        disc_write_manifest(manifest_path, "interrupted", device,
                            connected_name, monitor_cmd, have_bus_mode, bus_mode,
                            NULL, NULL, phase_meta);
    }
    disc_free_raw_phases(raw_phases);
    [g_ble disconnect];
    return 130;

disconnected:
    fprintf(stderr, "\n[ERROR] Connection lost during discover\n");
    if (dump_enabled) {
        disc_write_manifest(manifest_path, "disconnected", device,
                            connected_name, monitor_cmd, have_bus_mode, bus_mode,
                            NULL, NULL, phase_meta);
    }
    disc_free_raw_phases(raw_phases);
    return 1;
}

static void cli_print_usage(void) {
    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "  macos_obd_reader                                     Interactive mode\n");
    fprintf(stderr, "  macos_obd_reader scan                                Scan for BLE OBD devices\n");
    fprintf(stderr, "  macos_obd_reader query <device> <PIDs..>             Query OBD-II PIDs\n");
    fprintf(stderr, "  macos_obd_reader supported <device>                  List supported PIDs\n");
    fprintf(stderr, "  macos_obd_reader monitor <device> [secs] [--out FILE] [CAN_IDs]  CAN passive monitor\n");
    fprintf(stderr, "  macos_obd_reader raw <device> <command>              Send raw AT command\n");
    fprintf(stderr, "  macos_obd_reader detect <device>                     Auto-detect D-CAN vs PT-CAN\n");
    fprintf(stderr, "  macos_obd_reader discover <device> [--dump-dir DIR] Auto-discover PT-CAN signals\n");
    fprintf(stderr, "  macos_obd_reader p992 list                           List 992 XML channels + mappings\n");
    fprintf(stderr, "  macos_obd_reader p992 read <device> [channel...]     Read 992 profile channels once\n");
    fprintf(stderr, "  macos_obd_reader p992 watch <device> [...]           Stream 992 profile channels as NDJSON\n");
    fprintf(stderr, "  macos_obd_reader p992 fastlog <device> [...]         STN periodic-message fast logger\n");
    fprintf(stderr, "  macos_obd_reader p992 debug <device>                 Probe 992 routes and raw responses\n");
    fprintf(stderr, "  macos_obd_reader p992 bin-probe <device>             Probe exact AiM BIN-tail request containers\n");
    fprintf(stderr, "  macos_obd_reader p992 scan-did <device> [DID] [...]  Scan 0x700..0x7EF tx IDs for a DID\n");
    fprintf(stderr, "  macos_obd_reader p992 scan-did-range <device> [...]  Scan DID window on known responding ECU IDs\n");
    fprintf(stderr, "  macos_obd_reader p992 uds-read <device> <DID> [...]  Read one UDS DID with optional route/session\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "  <device>:   BLE name prefix match (e.g., OBDLink, vLinker)\n");
    fprintf(stderr, "  <CAN_IDs>:  Optional hex filter IDs (e.g., 0A5 1A0 316)\n");
    fprintf(stderr, "\n");
    fprintf(stderr, "  detect:    Passive listen 3s to determine bus type (D-CAN/PT-CAN)\n");
    fprintf(stderr, "  discover:  Interactive guided signal discovery + default capture dump\n");
    fprintf(stderr, "             Default dump dir: captures/YYYYMMDD_HHMMSS\n");
    fprintf(stderr, "  monitor:   Silent mode (no ACK) — safe for direct CAN bus tap\n");
    fprintf(stderr, "  query/supported/raw: Active mode (sends OBD-II requests)\n");
    fprintf(stderr, "  p992:      AiM 992 XML-backed reader. Custom DID channels expose raw + heuristic decode.\n");
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

    bool needs_ble = false;
    if (strcmp(subcmd, "scan") == 0 ||
        strcmp(subcmd, "query") == 0 ||
        strcmp(subcmd, "supported") == 0 ||
        strcmp(subcmd, "monitor") == 0 ||
        strcmp(subcmd, "raw") == 0 ||
        strcmp(subcmd, "detect") == 0 ||
        strcmp(subcmd, "discover") == 0) {
        needs_ble = true;
    } else if (strcmp(subcmd, "p992") == 0) {
        if (argc >= 3 &&
            strcmp(argv[2], "list") != 0 &&
            strcmp(argv[2], "help") != 0 &&
            strcmp(argv[2], "--help") != 0) {
            needs_ble = true;
        }
    }

    if (needs_ble) {
        g_ble = [[BLEManager alloc] init];
    }

    int ret = 0;

    if (strcmp(subcmd, "scan") == 0) {
        ret = cli_scan();
    } else if (strcmp(subcmd, "p992") == 0) {
        ret = cli_p992(argc, argv);
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
            fprintf(stderr, "[ERROR] Usage: monitor <device> [seconds] [--out FILE] [CAN_ID_hex...]\n");
            ret = 1;
        } else {
            int secs = 5;
            int filter_start = 3;  /* argv index where filter IDs begin */
            const char *filter_args[64];
            int filter_argc = 0;
            g_cli_monitor_out_path = NULL;
            /* If argv[3] looks like a number (seconds), consume it */
            if (argc >= 4) {
                char *endp;
                long val = strtol(argv[3], &endp, 10);
                if (*endp == '\0' && val > 0) {
                    secs = (int)val;
                    filter_start = 4;
                }
            }
            for (int i = filter_start; i < argc; i++) {
                if (strcmp(argv[i], "--out") == 0) {
                    if (i + 1 >= argc) {
                        fprintf(stderr, "[ERROR] --out requires a file path\n");
                        ret = 1;
                        break;
                    }
                    g_cli_monitor_out_path = argv[++i];
                } else if (strncmp(argv[i], "--out=", 6) == 0) {
                    g_cli_monitor_out_path = argv[i] + 6;
                } else if (filter_argc < (int)(sizeof(filter_args) / sizeof(filter_args[0]))) {
                    filter_args[filter_argc++] = argv[i];
                } else {
                    fprintf(stderr, "[ERROR] Too many monitor filter arguments\n");
                    ret = 1;
                    break;
                }
            }
            if (ret == 0) {
                ret = cli_monitor(argv[2], secs,
                                  filter_argc > 0 ? filter_args : NULL,
                                  filter_argc);
            }
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
            fprintf(stderr, "[ERROR] Usage: discover <device> [--dump-dir DIR]\n");
            ret = 1;
        } else {
            const char *dump_dir_override = NULL;
            for (int i = 3; i < argc; i++) {
                if (strcmp(argv[i], "--dump-dir") == 0) {
                    if (i + 1 >= argc) {
                        fprintf(stderr, "[ERROR] --dump-dir requires a directory path\n");
                        return 1;
                    }
                    dump_dir_override = argv[++i];
                } else if (strncmp(argv[i], "--dump-dir=", 11) == 0) {
                    dump_dir_override = argv[i] + 11;
                } else {
                    fprintf(stderr, "[ERROR] Unknown discover option: %s\n", argv[i]);
                    fprintf(stderr, "[ERROR] Usage: discover <device> [--dump-dir DIR]\n");
                    return 1;
                }
            }
            ret = cli_discover(argv[2], dump_dir_override);
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
