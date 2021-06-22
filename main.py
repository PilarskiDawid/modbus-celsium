import base64
from pymodbus.client.sync import ModbusTcpClient
from google.cloud import storage
import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery
from datetime import datetime, timedelta
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
import pandas_gbq

import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "auth_cels.json"

INCLUDED_REGISTERS = [1000, 1002, 1004, 1006, 1008, 1010, 1012, 1016, 1018, 1020, 1022, 1024, 1026, 1028, 1032, 1034,
                      1036, 1038, 1040, 1042, 1044, 1048, 1050, 1054, 1058, 1062, 1066, 1068, 1070, 1072, 1074, 1078,
                      1082, 1086, 1090, 1092, 1094, 1096, 1098, 1100, 1102, 1104, 1108, 1110, 1112, 1116, 1118, 1120,
                      1124, 1126, 1128, 1132, 1134, 1136, 1138, 1140, 1142, 1144, 1146, 1148, 1150, 1152, 1154, 1156,
                      1158, 1162, 1164, 1166, 1170, 1172, 1174, 1178, 1180, 1182, 1186, 1188, 1190, 1192, 1194, 1196,
                      1198, 1200, 1202, 1204, 1206, 1208, 1210, 1212, 1216, 1218, 1220, 1224, 1226, 1228, 1232, 1234,
                      1236, 1240, 1242, 1244, 1246, 1248, 1250, 1252, 1254, 1256, 1258, 1260, 1262, 1264, 1266, 1270,
                      1272, 1274, 1278, 1280, 1282, 1286, 1288, 1290, 1294, 1296, 1298, 1300, 1302, 1304, 1306, 1308,
                      1310, 1312, 1314, 1316, 1318, 1320, 1324, 1326, 1328, 1332, 1334, 1336, 1340, 1342, 1344, 1348,
                      1350, 1352, 1354, 1356, 1358, 1360, 1362, 1364, 1366, 1368, 1370, 1372, 1374, 1378, 1380, 1382,
                      1386, 1388, 1390, 1394, 1396, 1398, 1402, 1404, 1406, 1408, 1410, 1412, 1414, 1416, 1418, 1420,
                      1422, 1424, 1426, 1428, 1430]
DOUBLE_REGISTERS = [1050, 1054, 1058, 1062, 1074, 1078, 1082, 1086]

INT_REGISTERS = [1108, 1110, 1112, 1116, 1118, 1120, 1124, 1126, 1128, 1162, 1164, 1166, 1170, 1172, 1174, 1178, 1180,
                 1182, 1216, 1218, 1220, 1224, 1226, 1228, 1232, 1234, 1236, 1270, 1272, 1274, 1278, 1280, 1282, 1286,
                 1288, 1290, 1324, 1326, 1328, 1332, 1334, 1336, 1340, 1342, 1344, 1378, 1380, 1382, 1386, 1388, 1390,
                 1394, 1396, 1398, 1070, 1094, 1098, 1100, 1102, 1104, 1132, 1134, 1136, 1138, 1140, 1142, 1144, 1146,
                 1148, 1152, 1154, 1156, 1158, 1186, 1188, 1190, 1192, 1194, 1196, 1198, 1200, 1202, 1206, 1208, 1210,
                 1212, 1240, 1242, 1244, 1246, 1248, 1250, 1252, 1254, 1256, 1260, 1262, 1264, 1266, 1294, 1296, 1298,
                 1300, 1302, 1304, 1306, 1308, 1310, 1314, 1316, 1318, 1320, 1348, 1350, 1352, 1354, 1356, 1358, 1360,
                 1362, 1364, 1368, 1370, 1372, 1374, 1402, 1404, 1406, 1408, 1410, 1412, 1414, 1416, 1418]

TARGET_TABLE = 'awesome-aspect-267511.odczyty_skarzysko_EU.skarzysko_data_2'
project_id = 'awesome-aspect-267511'


def word_list_to_binary(words):
    bin_list = []

    for w in words:
        bin_list.append(
            '{0:016b}'.format(w)
        )

    full_binary = "".join(bin_list)

    return full_binary


def binaryToDecimal(binary):
    decimal, i, n = 0, 0, 0
    while (binary != 0):
        dec = binary % 10
        decimal = decimal + dec * pow(2, i)
        binary = binary // 10
        i += 1
    return decimal


def convert_double(full_binary):
    bin_number = full_binary
    result = 0
    for i in range(1, 52):
        # i + 12 --> od 13 do 64 bitu
        result = result + (float(bin_number[11 + i]) * (2 ** -i))
    exponent = bin_number[1:12]
    exponent = binaryToDecimal(int(exponent))

    fraction = (1 + result)
    tmp = 2 ** (exponent - 1023)
    if bin_number[0] == '0':
        sign = 0
    else:
        sign = 1

    result = ((-1) ** sign) * fraction * tmp
    return result


def hello_pubsub(event, context):
    path = 'dataset.csv'
    my_bucket = 'celsium-skarzysko'
    client = ModbusTcpClient('217.97.137.186')
    if client.connect():
        conn = True
        print("Connected")
    else:
        conn = False
        print("Not Connected")
    tab_value = []
    reg_name = ['mbHC1_info', 'mbHC1_Energy', 'mbHC1_Power', 'mbHC1_Volume', 'mbHC1_Flow', 'mbHC1_Ftemp', 'mbHC1_Rtemp',
                'mbHC2_info', 'mbHC2_Energy',
                'mbHC2_Power', 'mbHC2_Volume', 'mbHC2_Flow', 'mbHC2_Ftemp', 'mbHC2_Rtemp', 'mbHC3_info', 'mbHC3_Energy',
                'mbHC3_Power', 'mbHC3_Volume',
                'mbHC3_Flow', 'mbHC3_Ftemp', 'mbHC3_Rtemp', 'mbG1_info', 'mbG1_Volume', 'mbG1_Energy', 'mbG1_Hs',
                'mbG1_Denesity', 'mbG1_Temp',
                'mbG1_Pressure', 'mbG1_Flow', 'mbG2_info', 'mbG2_Volume', 'mbG2_Energy', 'mbG2_Hs', 'mbG2_Denesity',
                'mbG2_Temp', 'mbG2_Pressure',
                'mbG2_Flow', 'mbEC1_info', 'mbEC1_EPp', 'mbEC1_EPn', 'mbEC1_EQp', 'mbEC1_EQn', 'mbEC1_PL1', 'mbEC1_PL2',
                'mbEC1_PL3',
                'mbEC1_QL1', 'mbEC1_QL2', 'mbEC1_QL3', 'mbEC1_SL1', 'mbEC1_SL2', 'mbEC1_SL3', 'mbEC1_PFL1',
                'mbEC1_PFL2',
                'mbEC1_PFL3', 'mbEC1_VL1', 'mbEC1_VL2', 'mbEC1_VL3', 'mbEC1_IL1', 'mbEC1_IL2', 'mbEC1_IL3',
                'mbEC2_info', 'mbEC2_EPp', 'mbEC2_EPn',
                'mbEC2_EQp', 'mbEC2_EQn', 'mbEC2_PL1', 'mbEC2_PL2', 'mbEC2_PL3', 'mbEC2_QL1', 'mbEC2_QL2', 'mbEC2_QL3',
                'mbEC2_SL1', 'mbEC2_SL2', 'mbEC2_SL3', 'mbEC2_PFL1', 'mbEC2_PFL2', 'mbEC2_PFL3', 'mbEC2_VL1',
                'mbEC2_VL2', 'mbEC2_VL3', 'mbEC2_IL1',
                'mbEC2_IL2', 'mbEC2_IL3', 'mbEC3_info', 'mbEC3_EPp', 'mbEC3_EPn', 'mbEC3_EQp', 'mbEC3_EQn', 'mbEC3_PL1',
                'mbEC3_PL2', 'mbEC3_PL3',
                'mbEC3_QL1', 'mbEC3_QL2', 'mbEC3_QL3', 'mbEC3_SL1', 'mbEC3_SL2', 'mbEC3_SL3', 'mbEC3_PFL1',
                'mbEC3_PFL2', 'mbEC3_PFL3',
                'mbEC3_VL1', 'mbEC3_VL2', 'mbEC3_VL3', 'mbEC3_IL1', 'mbEC3_IL2', 'mbEC3_IL3', 'mbEC4_info', 'mbEC4_EPp',
                'mbEC4_EPn', 'mbEC4_EQp', 'mbEC4_EQn',
                'mbEC4_PL1', 'mbEC4_PL2', 'mbEC4_PL3', 'mbEC4_QL1', 'mbEC4_QL2', 'mbEC4_QL3', 'mbEC4_SL1', 'mbEC4_SL2',
                'mbEC4_SL3',
                'mbEC4_PFL1', 'mbEC4_PFL2', 'mbEC4_PFL3', 'mbEC4_VL1', 'mbEC4_VL2', 'mbEC4_VL3', 'mbEC4_IL1',
                'mbEC4_IL2', 'mbEC4_IL3', 'mbEC5_info', 'mbEC5_EPp',
                'mbEC5_EPn', 'mbEC5_EQp', 'mbEC5_EQn', 'mbEC5_PL1', 'mbEC5_PL2', 'mbEC5_PL3', 'mbEC5_QL1', 'mbEC5_QL2',
                'mbEC5_QL3', 'mbEC5_SL1',
                'mbEC5_SL2', 'mbEC5_SL3', 'mbEC5_PFL1', 'mbEC5_PFL2', 'mbEC5_PFL3', 'mbEC5_VL1', 'mbEC5_VL2',
                'mbEC5_VL3', 'mbEC5_IL1', 'mbEC5_IL2', 'mbEC5_IL3',
                'mbEC6_info', 'mbEC6_EPp', 'mbEC6_EPn', 'mbEC6_EQp', 'mbEC6_EQn', 'mbEC6_PL1', 'mbEC6_PL2', 'mbEC6_PL3',
                'mbEC6_QL1', 'mbEC6_QL2',
                'mbEC6_QL3', 'mbEC6_SL1', 'mbEC6_SL2', 'mbEC6_SL3', 'mbEC6_PFL1', 'mbEC6_PFL2', 'mbEC6_PFL3',
                'mbEC6_VL1', 'mbEC6_VL2', 'mbEC6_VL3',
                'mbEC6_IL1', 'mbEC6_IL2', 'mbEC6_IL3', 'mbRSN1_THDuL1', 'mbRSN1_THDuL2', 'mbRSN1_THDuL3',
                'mbRSN2_THDuL1', 'mbRSN2_THDuL2', 'mbRSN2_THDuL3']
    if conn:
        tmp = 0
        print("Start odczytu zmiennych z rejestrów ")
        for nr_rejestr in range(1000, 1431, 2):
            if nr_rejestr in INCLUDED_REGISTERS:
                try:
                    if nr_rejestr in DOUBLE_REGISTERS:
                        r_value = client.read_input_registers(nr_rejestr, 4)
                        binary_value = word_list_to_binary(r_value.registers)
                        value = convert_double(binary_value)

                        print(reg_name[tmp],nr_rejestr, value)
                    elif nr_rejestr in INT_REGISTERS:
                        r_value = client.read_input_registers(nr_rejestr, 2)
                        word = [r_value.registers[1], r_value.registers[0]]
                        bin = word_list_to_binary(word)
                        value = binaryToDecimal(int(bin))
                        if nr_rejestr == 1152:
                            value = value / 10
                    else:
                        r_value = client.read_input_registers(nr_rejestr, 2)
                        decoder = BinaryPayloadDecoder.fromRegisters(r_value.registers, Endian.Big,
                                                                     wordorder=Endian.Little)
                        value = decoder.decode_32bit_float()

                    now = datetime.now()
                    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
                    tab_value.append([dt_string, reg_name[tmp], value])
                    tmp = tmp + 1
                    # print("Zakończono odczyt zmiennych powodzeniem ")
                except Exception as e:
                    print('tutaj ', e)

        # print("Zapis wyników do pliku")
        try:
            df = pd.DataFrame(tab_value, columns=['Time', 'Register_name', 'Value'])
            df.to_gbq(destination_table=TARGET_TABLE, project_id=project_id, if_exists='append')
            return "Zapis zakończony powodzeniem"
        except Exception as e:
            print(e)
            return "Błąd zapisu"
    else:
        return "Błąd połączenia"


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    hello_pubsub('event', 'context')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
