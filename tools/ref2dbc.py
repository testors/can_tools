#!/usr/bin/env python3

import argparse
import os
import re
import sys
import zlib


FIELDS = [
    "Signal",
    "CAN_ID",
    "Unit",
    "StartBit",
    "BitLen",
    "Offset",
    "Factor",
    "Max",
    "Min",
    "Signed",
    "ByteOrder",
    "DLC",
]

ZLIB_HEADERS = {
    (0x78, 0x01),
    (0x78, 0x5E),
    (0x78, 0x9C),
    (0x78, 0xDA),
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Decode a VBOX ref-format file and emit a DBC file."
    )
    parser.add_argument("input", help="Path to the VBOX ref file")
    parser.add_argument(
        "-o",
        "--output",
        help="Output DBC path (defaults to <input>.dbc)",
    )
    return parser.parse_args()


def find_first_zlib(raw):
    for i in range(len(raw) - 1):
        if (raw[i], raw[i + 1]) in ZLIB_HEADERS:
            return i
    return -1


def try_decompress(block):
    for wbits in (15, -15, zlib.MAX_WBITS, -zlib.MAX_WBITS):
        try:
            text = zlib.decompress(block, wbits).decode("ascii", errors="replace")
            return text
        except Exception:
            pass
    return None


def decode_ref(filepath):
    with open(filepath, "rb") as f:
        raw = f.read()

    first_zlib = find_first_zlib(raw)
    if first_zlib < 2:
        raise ValueError("No supported zlib signature found in input")

    cut = first_zlib - 2
    data = raw[cut:]

    pos = 0
    blocks = []
    block_count = -1

    while pos < len(data) - 3:
        if data[pos] == 0x00 and pos + 2 < len(data):
            length = data[pos + 1]

            if (
                pos + 2 + length <= len(data)
                and (data[pos + 2], data[pos + 3]) in ZLIB_HEADERS
            ):
                block = data[pos + 2 : pos + 2 + length]
                text = try_decompress(block)
                if text is not None:
                    blocks.append(text)
                pos += 2 + length
                continue

            if length < 0x78 and block_count < 0:
                block_count = length
                pos += 2
                continue

        pos += 1

    return blocks, block_count


def parse_rows(blocks):
    rows = []
    for text in blocks:
        parts = text.rstrip(",").split(",")
        if len(parts) < 2:
            continue
        if parts[0].isdigit():
            continue
        padded = (parts + [""] * len(FIELDS))[: len(FIELDS)]
        row = dict(zip(FIELDS, padded))
        rows.append(row)
    return rows


def parse_int(value, default=0):
    text = value.strip()
    if not text:
        return default
    if text.lower().startswith("0x"):
        return int(text, 16)
    if re.fullmatch(r"[0-9a-fA-F]+", text) and re.search(r"[a-fA-F]", text):
        return int(text, 16)
    return int(float(text))


def parse_float(value, default=0.0):
    text = value.strip()
    if not text:
        return default
    return float(text)


def parse_signed(value):
    text = value.strip().lower()
    return text in {"1", "true", "yes", "signed", "-1"}


def parse_byte_order(value):
    text = value.strip().lower().replace("-", "").replace("_", "").replace(" ", "")
    if text in {"1", "intel", "little", "littleendian", "lsb"}:
        return 1
    if text in {"0", "motorola", "big", "bigendian", "msb"}:
        return 0
    raise ValueError(f"Unsupported ByteOrder value: {value!r}")


def format_number(value):
    if abs(value - round(value)) < 1e-12:
        return str(int(round(value)))
    return f"{value:.12g}"


def build_messages(rows):
    messages = {}

    for row in rows:
        can_id = parse_int(row["CAN_ID"])
        dlc = parse_int(row["DLC"], default=8)
        start_bit = parse_int(row["StartBit"])
        bit_len = parse_int(row["BitLen"])
        factor = parse_float(row["Factor"], default=1.0)
        offset = parse_float(row["Offset"], default=0.0)
        raw_min = parse_float(row["Min"], default=0.0)
        raw_max = parse_float(row["Max"], default=0.0)
        byte_order = parse_byte_order(row["ByteOrder"] or "1")
        sign = "-" if parse_signed(row["Signed"]) else "+"

        signal_name = row["Signal"].strip() or f"Signal_{start_bit}"
        signal_name = re.sub(r"[^A-Za-z0-9_]", "_", signal_name)
        if not signal_name or signal_name[0].isdigit():
            signal_name = f"SIG_{signal_name}"

        messages.setdefault(
            can_id,
            {
                "name": f"MSG_{can_id:03X}",
                "dlc": dlc,
                "signals": [],
            },
        )

        if dlc > messages[can_id]["dlc"]:
            messages[can_id]["dlc"] = dlc

        messages[can_id]["signals"].append(
            {
                "name": signal_name,
                "start_bit": start_bit,
                "bit_len": bit_len,
                "byte_order": byte_order,
                "sign": sign,
                "factor": factor,
                "offset": offset,
                "min": raw_min,
                "max": raw_max,
                "unit": row["Unit"].strip(),
            }
        )

    for message in messages.values():
        message["signals"].sort(key=lambda signal: (signal["start_bit"], signal["name"]))

    return dict(sorted(messages.items()))


def write_escaped(f, text):
    for ch in text:
        if ch in {'"', '\\'}:
            f.write("\\")
        f.write(ch)


def write_dbc(output_path, messages):
    with open(output_path, "w", encoding="utf-8", newline="\n") as f:
        f.write('VERSION ""\n\n')
        f.write("NS_ :\n")
        f.write("\tNS_DESC_\n")
        f.write("\tCM_\n")
        f.write("\tBA_DEF_\n")
        f.write("\tBA_\n")
        f.write("\tVAL_\n")
        f.write("\tCAT_DEF_\n")
        f.write("\tCAT_\n")
        f.write("\tFILTER\n")
        f.write("\tBA_DEF_DEF_\n")
        f.write("\tEV_DATA_\n")
        f.write("\tENVVAR_DATA_\n")
        f.write("\tSGTYPE_\n")
        f.write("\tSGTYPE_VAL_\n")
        f.write("\tBA_DEF_SGTYPE_\n")
        f.write("\tBA_SGTYPE_\n")
        f.write("\tSIG_TYPE_REF_\n")
        f.write("\tVAL_TABLE_\n")
        f.write("\tSIG_GROUP_\n")
        f.write("\tSIGTYPE_VALTYPE_\n")
        f.write("\tSG_MUL_VAL_\n\n")
        f.write("BS_: 500\n\n")
        f.write("BU_: VBOX\n\n")

        for can_id, message in messages.items():
            f.write(f"BO_ {can_id} {message['name']}: {message['dlc']} VBOX\n")
            for signal in message["signals"]:
                f.write(
                    f" SG_ {signal['name']} : "
                    f"{signal['start_bit']}|{signal['bit_len']}@{signal['byte_order']}{signal['sign']} "
                    f"({format_number(signal['factor'])},{format_number(signal['offset'])}) "
                    f"[{format_number(signal['min'])}|{format_number(signal['max'])}] "
                    f'"'
                )
                write_escaped(f, signal["unit"])
                f.write('" Vector__XXX\n')
            f.write("\n")


def main():
    args = parse_args()
    output_path = args.output
    if not output_path:
        stem, _ = os.path.splitext(args.input)
        output_path = stem + ".dbc"

    blocks, block_count = decode_ref(args.input)
    rows = parse_rows(blocks)
    if not rows:
        raise ValueError("No signal rows decoded from input")

    messages = build_messages(rows)
    write_dbc(output_path, messages)

    print(f"Decoded blocks: {len(blocks)}")
    if block_count >= 0:
        print(f"Block count field: {block_count}")
    print(f"Signals written: {len(rows)}")
    print(f"Messages written: {len(messages)}")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
