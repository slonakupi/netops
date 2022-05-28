import getpass
import threading
from netmiko import *
import textfsm
import pandas as pd
import re
from ciscoconfparse import CiscoConfParse
import time
import datetime
from jinja2 import Environment, FileSystemLoader
from IPython.display import clear_output  # очистка экрана Jupiter
import os
import ipaddress

# import logging
# import numpy as nm
# import sys
# from pprint import pprint
# from queue import Queue


# логин и пароль для оборудования
USER = input('Username: ')
PASSWORD = getpass.getpass(prompt='Enter password: ')

# переменные

# paths
cfg_dir = 'cfg'
result_dir = 'results'
cfgbase_dir = 'cfg\\base'
macerr_dir = 'cfg\\macerr'
maclist_dir = 'cfg\\maclist'
mactable_dir = 'xlsx\\mactable'
strangeports_dir = 'xlsx\\mactable\\strangeports'
data_dir = 'xlsx'
templates_dir = 'templates'
txtfsm_dir = 'templates\\txtfsm'
ntc_templates_dir = 'templates\\ntc-templates'

TERM_LE_CMD = {'cisco_asa': '',
               'cisco_ios': 'terminal length 0',
               'poligon': 'terminal length 0',
               'huawei': 'screen-length 0',
               'hp_comware': 'screen-length disable'}

SHRUN_CMD = {'cisco_asa': 'show run',
             'cisco_ios': 'show run',
             'poligon': 'show running-config',
             'huawei': 'display cur',
             'hp_comware': 'display cur'}

SAVECFG_CMD = {'cisco_asa': 'write',
               'cisco_ios': 'do write',
               'poligon': '',
               'huawei': 'run save\nY',
               'hp_comware': 'save force'}

SHVER_CMD = {'cisco_asa': 'show ver',
             'cisco_ios': 'show ver',
             'poligon': 'show version',
             'huawei': 'display version',
             'hp_comware': 'display version'}

INV_CMD = {'cisco_asa': 'cluster exec show inventory',
           'cisco_ios': 'show inventory',
           'poligon': '',
           'huawei': 'dis dev manu',
           'hp_comware': 'dis dev manu'}

SFP_CMD = {'cisco_asa': '',
           'cisco_ios': '',
           'poligon': '',
           'huawei': 'display transceiver'}

PATCH_CMD = {'cisco_asa': '',
             'cisco_ios': '',
             'poligon': '',
             'huawei': 'display patch-information'}

MAC_CMD = {'cisco_asa': 'show interface ip br',
           'cisco_ios': 'show mac address-table',
           'poligon': 'show mac address-table',
           'huawei': 'display mac-address',
           'hp_comware': 'display mac-address'}

MAC_CMD2 = {'cisco_asa': '',
            'cisco_ios': '',
            'huawei': '',
            'hp_comware': 'display port-security mac-address security'}

SH_IP_INT_BR_CMD = {'cisco_asa': 'show interface ip br',
                    'cisco_ios': 'show ip int br',
                    'poligon': 'show ip int br',
                    'huawei': 'dis ip int br',
                    'hp_comware': 'dis ip int br'}

SH_CDP_NE_CMD = {'cisco_asa': '',
                 'cisco_ios': 'show cdp neighbors',
                 'poligon': '',
                 'huawei': '',
                 'hp_comware': ''}

SH_LLDP_NE_CMD = {'cisco_asa': '',
                  'cisco_ios': 'show lldp neighbors',
                  'poligon': 'show lldp neighbors',
                  'huawei': 'display lldp neighbor brief',
                  'hp_comware': 'display lldp neighbor-information list'}

SH_INT_DESC_CMD = {'cisco_asa': '',
                   'cisco_ios': 'show interfaces description',
                   'poligon': 'show interface brief',
                   'huawei': 'display interface description',
                   'hp_comware': ''}

MACLIST_PARSE_TEMPLATE = {'cisco_asa': '',
                          'cisco_ios': '{path}\\cisco_ios_show_mac-address-table.txtfsm'.format(path=txtfsm_dir),
                          'poligon': '{path}\\poligon_maclist.txtfsm'.format(path=txtfsm_dir),
                          'huawei': '{path}\\huawei_maclist.txtfsm'.format(path=txtfsm_dir),
                          'hp_comware': '{path}\\hp_comware_maclist_sticky.txtfsm'.format(path=txtfsm_dir)}

# columns
DATE_COLUMN = 'DATE'
USER_COLUMN = 'USR_LOCAL'
PASSWORD_COLUMN = 'PWD_LOCAL'
SECRET_COLUMN = 'SCRT_LOCAL'
EXEC_COLUMN = 'Exec'
IP_COLUMN = 'IP_DEV'
DATA_COLUMN = 'DATAFILE'
SEGMENT_COLUMN = 'Segment'
HOSTNAME_COLUMN = 'Hostname'
PROFILE_COLUMN = 'PROFILE'
CONMODE_COLUMN = 'CON_MODE'
# FILENAME_COLUMN = 'Filename'
COMMENT_COLUMN = 'Comment'
CONFIG_COLUMN = 'CFG'
PARSE_COLUMN = 'Parse'
SERIAL_COLUMN = 'Serial'
HW_COLUMN = 'Hardware'
VERSION_COLUMN = 'Version'
VERSIONFULL_COLUMN = 'VersionFULL'
PATCH_COLUMN = 'Patch'
INV_COLUMN = 'Inventory'
IP_INT_COLUMN = 'IP_Interfaces'
SFP_COLUMN = 'Transciever (Huawei)'
CMD_TEMPLATE_COLUMN = 'CMD_template'
CMDCFG_COLUMN = 'CMDCFG'
CDP_COLUMN = 'CDP NE'
LLDP_COLUMN = 'LLDP NE'
INT_DESC_COLUMN = 'INT DESCR'
RESULT_COLUMN = 'RESULT'

DHCPR1_COLUMN = 'DHCP_relay_1'
DHCPR2_COLUMN = 'DHCP_relay_2'

IF_COLUMN = 'IF'
VLAN_COLUMN = 'VLAN'
VLAN_OLD_COLUMN = 'VLAN_OLD'
VLAN_NEW_COLUMN = 'VLAN_NEW'

VLNAME_COLUMN = 'VLAN_NAME'
VLIP_COLUMN = 'VLAN_IP'
VLSUB_COLUMN = 'VLAN_SUBNET'
VLDESC_COLUMN = 'VLAN_DESC'

reg = re.compile('[^a-zA-Z0-9\-\_\.]')  # допустимые символы в hostname A-z 0-9 - _ .
reg_mac = re.compile('[^a-zA-Z0-9]')  # допустимые символы в MAC A-z 0-9
reg_if = re.compile('[^GF0-9\/]')  # допустимые символы в Interface G,F 0-9 и /
reg_list_if = re.compile('[^A-Za-z0-9\/\,\ ]')  # допустимые символы в Interface G,F 0-9 и /
reg_list_vlan = re.compile('[^A-Za-z0-9\,]')  # допустимые символы в списке vlan
reg_vlan = re.compile('[^0-9]')  # допустимые символы в VLAN 0-9
reg_image = re.compile('[\/]')  # исключаемые символы
reg_spaces = re.compile('\s')  # пробельные символы
reg_blank_lines = re.compile('^\s*$')


class color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


# нормализация
def norm(rx, str):
    val = rx.sub('', str)
    return val


mac_norm = lambda x: reg_mac.sub('', x).lower()  # MAC к обычному виду без точек и дефисов в нижнем регистре
mac_cisco = lambda x: mac_norm(x)[0:4] + '.' + mac_norm(x)[4:8] + '.' + mac_norm(x)[8:12]  # MAC к виду конфига Cisco
mac_huawei = lambda x: mac_norm(x)[0:4] + '-' + mac_norm(x)[4:8] + '-' + mac_norm(x)[8:12]  # MAC к виду конфига Huawei

vlan_norm = lambda x: reg_vlan.sub('', str(x))  # нормализация VLAN
list_vlan_norm = lambda x: reg_list_vlan.sub('', str(x))

pdcol2str_newline = lambda x: '\n'.join(
    list(x.dropna()))  # перевод колонки dataframe в строку с разделителем - новой строкой
pdcol2str_zpt = lambda x: ', '.join(list(x.dropna()))


def pdcol2str(data_pd, delimeter):  # перевод колонки dataframe в строку с разделителем delimeter
    out = '{}'.format(delimeter).join(list(data_pd.dropna()))
    return out


pdnan2none = lambda x: x.where((pd.notnull(x)), None)  # замена NaN на None в dataframe

clear = lambda: clear_output(wait=True)  # очистка вывода Jupiter


def if_norm(IF):
    IF = re.sub(r'\b(XGE)((?:\d+\/)*\d)', r'XGigabitEthernet\2', str(IF))  # huawei
    IF = re.sub(r'\b(Te)((?:\d+\/)*\d)', r'TenGigabitEthernet\2', str(IF))
    IF = re.sub(r'\b(GE|Gi|Gig)\s?((?:\d+\/)*\d)', r'GigabitEthernet\2', str(IF))
    IF = re.sub(r'\b(Fa)((?:\d+\/)*\d)', r'FastEthernet\2', str(IF))
    IF = re.sub(r'\b(g)((?:\d+\/)*\d)', r'GigaEthernet\2', str(IF))  # poligon
    IF = re.sub(r'\b(Eth)((?:\d+\/)*\d)', r'Ethernet\2', str(IF))  # old ones
    return IF


def pd_IF_norm(DATAFRAME, IFCOLUMN):
    try:
        # замена коротких (неправильных наименований интерфейсов, например в выводе Huawei)
        DATAFRAME[IFCOLUMN] = DATAFRAME[IFCOLUMN].replace(r'(\b.*?)(GE)(.*?\b)', r'\1GigabitEthernet\3',
                                                          regex=True)  # *GE* >> *GigabitEthernet*

    except Exception as e:
        print('pd_IF_norm(DATAFRAME, IFCOLUMN)', e)

    return DATAFRAME


def yes_or_no(question):
    reply = str(input(question + ' (y/n): ')).lower().strip()
    if reply[:1] == 'y':
        return True
    elif reply[:1] == 'n':
        return False
    else:
        return yes_or_no("Uhhhh... please enter ")


def press_any_key_or_quit():
    reply = str(input('Press ENTER to continue or "q" to stop the operation')).lower().strip()
    if reply[:1] == 'q':
        return 'quit'
    if reply[:1] == '0':
        return 'go_unlim'
    else:
        return 'go_step'


def parse_cfg(CONFIG, PARENTTXT):
    try:
        conf_pd = pd.DataFrame()

        parse = CiscoConfParse(CONFIG.splitlines())  # Распарсить конфиг

        # Return a list of all interfaces matching pattern
        parser_obj = parse.find_objects(PARENTTXT)

        if parser_obj == None:
            return False

        for i, obj in enumerate(parser_obj):
            child_list = parse.find_all_children('^' + obj.text + '$')  # найти весь child конфиг объекта parent
            child_text = '\n'.join(child_list)

            conf_pd.loc['cfg', obj.text] = child_text  # найти весь child конфиг объекта parent

            # print(obj.text)
            # print(i)

        return conf_pd

    except Exception as e:
        print('parse_cfg(CONFIG, PARENTTXT)', e)


def parse_cfg_w_child(CONFIG, PARENTTXT, CHILDTXT):
    try:
        conf_pd = pd.DataFrame()

        parse = CiscoConfParse(CONFIG.splitlines())  # Распарсить конфиг

        # Return a list of all interfaces matching pattern
        parser_obj = parse.find_objects_w_child(PARENTTXT, CHILDTXT)

        if parser_obj == None:
            return False

        for i, obj in enumerate(parser_obj):
            child_list = parse.find_all_children('^' + obj.text + '$')  # найти весь child конфиг объекта parent
            child_text = '\n'.join(child_list)

            conf_pd.loc['cfg', obj.text] = child_text  # найти весь child конфиг объекта parent

            # print(obj.text)
            # print(i)

        return conf_pd

    except Exception as e:
        print('parse_cfg_w_child(CONFIG, PARENTTXT, CHILDTXT)', e)


# обработка вывода команд huawei по шаблону template
def list_parse(cfg, templatefile):
    try:
        # print('1')
        template = open(templatefile)
        fsm = textfsm.TextFSM(template)
        result = fsm.ParseText(cfg)
        resultpd = pd.DataFrame(result, columns=fsm.header)

        # если парсер не подошел - таблица пустая - то попробовать другой(следующий) шаблон

        i = 2
        while resultpd.empty:
            # print(i)
            templatefile_new = templatefile + str(i)
            template = open(templatefile_new)
            fsm = textfsm.TextFSM(template)
            result = fsm.ParseText(cfg)
            resultpd = pd.DataFrame(result, columns=fsm.header)
            i += 1



    except Exception as e:
        print('list_parse(cfg, templatefile)', e)

    return resultpd


# проверка несоответствия имени хоста в файле и девайсе
def hostname_mismatch(hf, hd):
    if hf != hd:
        print('WARNING: HOSTNAME MISMATCH')
        print('    Device: {0}'.format(hd))
        print('    File:   {0}'.format(hf))


# прочитать информацию из файла
def rffile(path):
    filedata = None

    if os.path.exists(path):
        with open(path, 'r') as file:  # прочитать файл
            filedata = file.read()

    return filedata


# прочитать информацию из файла и удалить файл после
def rffile_del(path):
    filedata = None

    if os.path.exists(path):
        with open(path, 'r') as file:  # прочитать файл
            filedata = file.read()
        os.remove(path)

    return filedata


# собрать информацию из файлов конфигурации в dataframe
def filedata_to_pd(dataframe):
    try:
        for index in dataframe.index:

            HOSTNAME = dataframe[HOSTNAME_COLUMN][index]  # первоначальное присваивание
            IP_DEV = dataframe[IP_COLUMN][index]

            print(IP_DEV, HOSTNAME)  # выврд информации
            clear()  # очистка экрана jupyter

            # прочитать hostname и заменить данными из файла, есл он не пустой
            data = rffile('{path}\\{ip}.{ftype}'.format(path=cfgbase_dir, ip=IP_DEV,
                                                        ftype='dns'))  # считать правильное имя из файла
            # print(data)

            if data != None and data == data:
                HOSTNAME = data

            dataframe.loc[index, HOSTNAME_COLUMN] = HOSTNAME

            # прочитать config
            data_cfg = rffile('{path}\\{filename}.{ftype}'.format(path=cfgbase_dir, filename=IP_DEV, ftype='cfg'))
            dataframe.loc[index, 'CFG'] = data_cfg if data_cfg != None else None

            # прочитать version
            data_ver = rffile('{path}\\{filename}.{ftype}'.format(path=cfgbase_dir, filename=IP_DEV, ftype='ver'))
            dataframe.loc[index, 'VER'] = data_ver if data_ver != None else dataframe.loc[index, 'VER']
            # прочитать inventory
            data_inv = rffile('{path}\\{filename}.{ftype}'.format(path=cfgbase_dir, filename=IP_DEV, ftype='inv'))
            dataframe.loc[index, 'INV'] = data_inv if data_inv != None else dataframe.loc[index, 'INV']
            # прочитать error
            data_err = rffile('{path}\\{ip_dev}.{ftype}'.format(path=cfgbase_dir, ip_dev=IP_DEV, ftype='err'))
            dataframe.loc[index, 'Error'] = data_err if data_err != None else dataframe.loc[index, 'Error']

            # прочитать conmode
            conmode = rffile('{path}\\{ip_dev}.{ftype}'.format(path=cfgbase_dir, ip_dev=IP_DEV, ftype='conmode'))
            dataframe.loc[index, 'CON_MODE'] = conmode if conmode != None else dataframe.loc[index, 'CON_MODE']

            ##прочитать maclist
            # data_maclist = rffile('{path}\\{filename}.{ftype}'.format(path=maclist_dir,filename = HOSTNAME + '_mac', ftype='list'))
            # dataframe.loc[index, 'MACLIST Head'] = data_maclist[:2] if data_maclist != None else None

            # прочитать mac error
            data_err = rffile('{path}\\{ip_dev}.{ftype}'.format(path=macerr_dir, ip_dev=IP_DEV, ftype='macerr'))
            dataframe.loc[index, 'MAC Error'] = data_err if data_err != None else dataframe.loc[index, 'MAC Error']

            # прочитать cdp ne
            data_cdp = rffile('{path}\\{ip_dev}.{ftype}'.format(path=cfgbase_dir, ip_dev=IP_DEV, ftype='cdp_ne'))
            dataframe.loc[index, CDP_COLUMN] = data_cdp if data_err != None else dataframe.loc[index, CDP_COLUMN]

            # прочитать lldp ne
            data_lldp = rffile('{path}\\{ip_dev}.{ftype}'.format(path=cfgbase_dir, ip_dev=IP_DEV, ftype='lldp_ne'))
            dataframe.loc[index, LLDP_COLUMN] = data_lldp if data_err != None else dataframe.loc[index, LLDP_COLUMN]

            # прочитать if description
            data_if_desc = rffile('{path}\\{ip_dev}.{ftype}'.format(path=cfgbase_dir, ip_dev=IP_DEV, ftype='if_desc'))
            dataframe.loc[index, INT_DESC_COLUMN] = data_if_desc if data_err != None else dataframe.loc[
                index, INT_DESC_COLUMN]

            data_cfg = None
            data_ver = None
            data_inv = None
            data_err = None
            data_maclist = None
            data_cdp = None
            data_lldp = None
            data_if_desc = None

    except Exception as e:
        print('\nError occured with', HOSTNAME)
        print('filedata_to_pd(dataframe)', e)
        dataframe.loc[index, 'Error'] = e

    return dataframe


def result_to_pd(dataframe, FILENAME):
    try:
        NOWSTR = datetime.datetime.now().strftime(
            '%Y-%m-%d_%H-%M-%S')  # получить текущие дату и время для имени файла результатов

        IF_RESULT_PD = pd.DataFrame()

        for i in dataframe.index:
            try:
                IP_DEV = dataframe.loc[i, IP_COLUMN]
                # прочитать результаты из файла
                data_result = rffile_del(
                    '{path}\\{ip_dev}.{ftype}'.format(path=cfgbase_dir, ip_dev=IP_DEV, ftype='result'))
                if data_result != None:
                    dataframe.loc[i, RESULT_COLUMN] = data_result

                if dataframe.loc[i, DATA_COLUMN] == dataframe.loc[i, DATA_COLUMN] and dataframe.loc[
                    i, DATA_COLUMN] != None:  # поле с файлом данных не пустое
                    if os.path.exists('{path}\\{ip_dev}_if_result.{ftype}'.format(path=cfgbase_dir, ip_dev=IP_DEV,
                                                                                  ftype='xlsx')):  # if file exists
                        filepath = '{path}\\{ip_dev}_if_result.{ftype}'.format(path=cfgbase_dir, ip_dev=IP_DEV,
                                                                               ftype='xlsx')
                        if_data_result = pd.read_excel(filepath, encoding='cp1251')
                        IF_RESULT_PD = IF_RESULT_PD.append(if_data_result, ignore_index=True)
                        os.remove(filepath)  # DELETE file after successful read

            except Exception as ee:
                print('\nError occured with', IP_DEV)
                print('result_to_pd(dataframe) 2nd cycle', ee)
                dataframe.loc[index, 'Error'] = ee

        # print('Saving file ' + 'results\\{0}_{1}_res.xlsx'.format(FILENAME,NOWSTR))
        if not dataframe.empty:
            print('Saving file results\\{0}_{1}_res.xlsx'.format(FILENAME, NOWSTR))
            dataframe.to_excel('results\\{0}_{1}_res.xlsx'.format(FILENAME, NOWSTR), encoding='cp1251',
                               index=False)  # save to ls file

        if not IF_RESULT_PD.empty:
            print('Saving file results\\{0}_{1}_ifsummary_res.xlsx'.format(FILENAME, NOWSTR))
            IF_RESULT_PD.to_excel('results\\{0}_{1}_ifsummary_res.xlsx'.format(FILENAME, NOWSTR), encoding='cp1251',
                                  index=False)  # save to ls file

    except Exception as e:
        print('\nError occured with', IP_DEV)
        print('result_to_pd(dataframe)', e)
        dataframe.loc[index, 'Error'] = e

    return dataframe


# получение конфига с устройства
# через потоки, параллельное считывание
def get_config_th(**kwargs):
    HOSTNAME = kwargs[HOSTNAME_COLUMN]
    tic = time.time()  # начало выполнения
    IP = kwargs[IP_COLUMN]

    if kwargs['attempts'] <= 0:
        print('Reached maximum number of attemps')
        return False

    # поменять профиль для telnet
    if kwargs[PROFILE_COLUMN].lower() == 'poligon':
        CON_PROFILE = 'cisco_ios'
    else:
        CON_PROFILE = kwargs[PROFILE_COLUMN]  # default ssh

    if kwargs[CONMODE_COLUMN] == 'telnet':
        CON_PROFILE += '_telnet'

    if IP != IP or kwargs[PROFILE_COLUMN] != kwargs[PROFILE_COLUMN] or (kwargs[EXEC_COLUMN] != 'X' and kwargs[
        EXEC_COLUMN] != 'D'): return kwargs  # NaN check, проверка пустых строк + проверка строк для выполнения - ничего с ними не делать, пропускать

    DEVICE_PARAMS = {'device_type': CON_PROFILE,
                     'ip': IP,
                     'username': USER if kwargs[USER_COLUMN] != kwargs[USER_COLUMN] else kwargs[USER_COLUMN],
                     'password': PASSWORD if kwargs[PASSWORD_COLUMN] != kwargs[PASSWORD_COLUMN] else kwargs[
                         PASSWORD_COLUMN],
                     'secret': PASSWORD if kwargs[SECRET_COLUMN] != kwargs[SECRET_COLUMN] else kwargs[SECRET_COLUMN]}
    # 'global_delay_factor':1}

    try:

        NOWSTR = datetime.datetime.now().strftime(
            '_%Y-%m-%d_%H-%M-%S')  # получить текущие дату и время для имени файла результатов

        if HOSTNAME == None or not (HOSTNAME == HOSTNAME):  # если HOSTNAME пустое или Nan
            HOSTNAME = 'unknown'

        print('{:<40s}{:<16s}{:<40s}'.format(HOSTNAME, IP, 'Connection to device'))

        with ConnectHandler(**DEVICE_PARAMS) as ssh:

            HOSTNAME = ssh.find_prompt()
            HOSTNAME = reg.sub('', HOSTNAME)  # нормализация hostname

            # проверка несоответствия имени хоста в файле и девайсе
            hostname_mismatch(kwargs[HOSTNAME_COLUMN], HOSTNAME)

            ssh.enable()  # переход в привелегированный режим

            ssh.send_command(TERM_LE_CMD[kwargs[PROFILE_COLUMN]])  # неограниченный вывод

            ###BACKUP

            kwargs[HOSTNAME_COLUMN] = HOSTNAME  # записать hostname в dataframe
            with open('{path}\\{filename}.ip'.format(path=cfgbase_dir, filename=HOSTNAME), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write(kwargs[IP_COLUMN])  # записать в файл
            with open('{path}\\{filename}.dns'.format(path=cfgbase_dir, filename=IP), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write(HOSTNAME)  # записать в файл           

            kwargs[CONFIG_COLUMN] = ssh.send_command(SHRUN_CMD[kwargs[PROFILE_COLUMN]])  # отправить команду show run
            with open('{path}\\{filename}.cfg'.format(path=cfgbase_dir, filename=IP), 'w',
                      encoding='utf-8') as file:  # открыть файл базы данных конфигов
                file.write(kwargs[CONFIG_COLUMN])  # записать в файл
            '''
            kwargs[VERSIONFULL_COLUMN] = ssh.send_command(SHVER_CMD[kwargs[PROFILE_COLUMN]])  # отправить команду show ver
            with open('{path}\\{filename}.ver'.format(path = cfgbase_dir, filename = IP), 'w', encoding='utf-8') as file:  # открыть файл базы данных конфигов
                file.write(kwargs[VERSIONFULL_COLUMN])  # записать в файл                
                
            kwargs[INV_COLUMN] = ssh.send_command(INV_CMD[kwargs[PROFILE_COLUMN]])  # отправить команду show inventory
            with open('{path}\\{filename}.inv'.format(path = cfgbase_dir, filename = IP), 'w', encoding='utf-8') as file:  # открыть файл базы данных конфигов
                file.write(kwargs[INV_COLUMN])  # записать в файл\

            #kwargs[IP_INT_COLUMN] = ssh.send_command(SH_IP_INT_BR_CMD[kwargs[PROFILE_COLUMN]])  # отправить команду show ip int brief
            #with open('{path}\\{filename}.if_ip'.format(path = cfgbase_dir, filename = IP), 'w', encoding='utf-8') as file:  # открыть файл базы данных конфигов
            #    file.write(kwargs[IP_INT_COLUMN])  # записать в файл
                
            if kwargs[PROFILE_COLUMN] == 'cisco_ios':
            kwargs[CDP_COLUMN] = ssh.send_command(SH_CDP_NE_CMD[kwargs[PROFILE_COLUMN]])  # отправить команду show cdp ne
            with open('{path}\\{filename}.cdp_ne'.format(path = cfgbase_dir, filename = IP), 'w', encoding='utf-8') as file:  # открыть файл базы данных конфигов
                file.write(kwargs[CDP_COLUMN])  # записать в файл

            kwargs[LLDP_COLUMN] = ssh.send_command(SH_LLDP_NE_CMD[kwargs[PROFILE_COLUMN]])  # отправить команду show lldp ne
            with open('{path}\\{filename}.lldp_ne'.format(path = cfgbase_dir, filename = IP), 'w', encoding='utf-8') as file:  # открыть файл базы данных конфигов
                file.write(kwargs[LLDP_COLUMN])  # записать в файл
                
            kwargs[INT_DESC_COLUMN] = ssh.send_command(SH_INT_DESC_CMD[kwargs[PROFILE_COLUMN]]) # отправить команду show int description          
            with open('{path}\\{filename}.if_desc'.format(path = cfgbase_dir, filename = IP), 'w', encoding='utf-8') as file:  # открыть файл базы данных конфигов
                file.write(kwargs[INT_DESC_COLUMN])  # записать в файл
            '''
            print(color.GREEN + '{:<40s}{:<16s}{:<40s}{:<15s}'.format(HOSTNAME, IP, 'Get data', 'OK') + color.END)

            with open('{path}\\{filename}.err'.format(path=cfgbase_dir, filename=IP), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write('no errors')  # записать в файл

            with open('{path}\\{filename}.conmode'.format(path=cfgbase_dir, filename=IP), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write('ssh')  # записать в файл   

    except Exception as e:
        print(color.RED + '{:<40s}{:<16s}{:<100s}'.format(HOSTNAME, IP, 'get_config_th() error' + str(e)) + color.END)

        if kwargs[CONMODE_COLUMN] != 'telnet' and ("timed-out" in str(e)) and ("WinError" in str(e)):
            print(color.YELLOW + '{:<40s}{:<16s}{:<40s}'.format(HOSTNAME, IP, 'Trying telnet connection') + color.END)
            kwargs[CONMODE_COLUMN] = 'telnet'

            with open('{path}\\{filename}.conmode'.format(path=cfgbase_dir, filename=IP), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write('telnet')  # записать в файл

            get_config_th(**kwargs)


        else:
            kwargs['attempts'] = kwargs['attempts'] - 1
            kwargs['Error'] = e
            with open('{path}\\{filename}.err'.format(path=cfgbase_dir, filename=IP), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write(str(e))  # записать в файл

            get_config_th(**kwargs)

    return kwargs


# поиск информации на устройстве по MAC адресу

def getmacinfo(**kwargs):
    # env = Environment(loader=FileSystemLoader('templates'),trim_blocks=True) #templates directory
    IP = kwargs[IP_COLUMN]

    if kwargs[PROFILE_COLUMN].lower() == 'poligon':
        CON_PROFILE = 'cisco_ios'
    else:
        CON_PROFILE = kwargs[PROFILE_COLUMN]  # default ssh

    if kwargs[CONMODE_COLUMN] == 'telnet':
        CON_PROFILE += '_telnet'

    if kwargs['attempts'] <= 0:
        print('Reached maximum number of attemps')
        return False

    print('\nConnection to device\t{}'.format(kwargs[IP_COLUMN]))

    DEVICE_PARAMS = {'device_type': CON_PROFILE,
                     'ip': kwargs[IP_COLUMN],
                     'username': USER if kwargs[USER_COLUMN] != kwargs[USER_COLUMN] else kwargs[USER_COLUMN],
                     'password': PASSWORD if kwargs[PASSWORD_COLUMN] != kwargs[PASSWORD_COLUMN] else kwargs[
                         PASSWORD_COLUMN],
                     'secret': PASSWORD if kwargs[SECRET_COLUMN] != kwargs[SECRET_COLUMN] else kwargs[SECRET_COLUMN]}
    # 'global_delay_factor':1}

    try:

        with ConnectHandler(**DEVICE_PARAMS) as ssh:

            # print('1')
            HOSTNAME = ssh.find_prompt()
            # print('2')
            HOSTNAME = reg.sub('', HOSTNAME)  # нормализация hostname

            ###BACKUP

            ssh.enable()  # привелигированный режим
            # print('3')
            ssh.send_command(TERM_LE_CMD[kwargs[PROFILE_COLUMN]])
            # print('4')

            MAC_COMMAND = MAC_CMD[kwargs[PROFILE_COLUMN]]

            if kwargs['Model'] == 'WS-C3524-XL' or kwargs['Model'] == 'WS-C3548-XL':  # исключение для CISCO 3548 и 3524
                MAC_COMMAND = 'show mac'

            kwargs['MAC_TABLE'] = ssh.send_command(MAC_COMMAND)  # получить вывод команды dis mac-address
            # print('5')

            # kwargs['CONFIG'] = ssh.send_command(SHRUN_CMD[kwargs[PROFILE_COLUMN]])  # получить конфиг отправить команду show run
            if kwargs[PROFILE_COLUMN] == 'hp_comware':  # если железяка - hp
                kwargs['MAC_TABLE'] = kwargs['MAC_TABLE'] + '\n' + (ssh.send_command(
                    MAC_CMD2[kwargs[PROFILE_COLUMN]]))  # получить вывод команды dis por-sec mac-address sec

            NOWSTR = datetime.datetime.now().strftime(
                '_%Y-%m-%d_%H-%M-%S')  # получить текущие дату и время для имени файла результатов
            # print('6')
            with open('{path}\\{filename}.ip'.format(path=cfgbase_dir, filename=HOSTNAME), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write(kwargs[IP_COLUMN])  # записать в файл

            with open('{path}\\{filename}.dns'.format(path=cfgbase_dir, filename=IP), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write(HOSTNAME)  # записать в файл

            # with open('{path}\\{filename}.cfg'.format(path = cfgbase_dir, filename = IP), 'w', encoding='utf-8') as file:  # открыть файл базы данных конфигов
            #    file.write(kwargs['CONFIG'])  # записать в файл
            #

            with open('{path}\\{filename}_mac.list'.format(path=maclist_dir, filename=IP),
                      'w') as file:  # запись вывода mac-адресов
                file.write(kwargs['MAC_TABLE'])  # записать в файл

            # таблица MAC в формате pandas dataframe
            # maclist_pd = list_parse(kwargs['MAC_TABLE'], MACLIST_PARSE_TEMPLATE[kwargs[PROFILE_COLUMN]])

            # нормализация MAC адресов
            # for i,mac in enumerate(maclist_pd['MAC']):
            #    maclist_pd.loc[i, 'MAC'] = mac_norm(mac)

            # maclist_pd.to_excel('xlsx\\mactable\{0}_mactable.xlsx'.format(kwargs[HOSTNAME_COLUMN]), encoding='cp1251',index=False)  # save to xlsx file

            print(color.GREEN + 'MAC table\t{0}\t{1}\t\t\tOK'.format(HOSTNAME, kwargs[IP_COLUMN]) + color.END)

            # проверка несоответствия имени хоста в файле и девайсе
            hostname_mismatch(kwargs[HOSTNAME_COLUMN], HOSTNAME)

            with open('{path}\\{filename}.err'.format(path=cfgbase_dir, filename=IP), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write('no errors')  # записать в файл


    except Exception as e:
        print(color.RED + '{:<40s}{:<16s}{:<40s}'.format(HOSTNAME, IP, 'getmacinfo() error' + str(e)) + color.END)

        if kwargs[CONMODE_COLUMN] != 'telnet' and ("timed-out" in str(e)):
            print(color.YELLOW + '{:<40s}{:<16s}{:<40s}'.format(HOSTNAME, IP, 'Trying telnet connection') + color.END)
            kwargs[CONMODE_COLUMN] = 'telnet'
            getmacinfo(**kwargs)

        else:
            # print('\nError occured with', kwargs[IP_COLUMN])
            # print('getmacinfo(**kwargs)',e)
            # return False
            kwargs['attempts'] = kwargs['attempts'] - 1  # уменьшить число попыток
            getmacinfo(**kwargs)

            with open('{path}\\{filename}.err'.format(path=cfgbase_dir, filename=IP), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write(str(e))  # записать в файл

    # return maclist_pd


def maclist_to_excel(DEVICES_RUN):
    tic = time.time()  # начало выполнения

    NOWSTR = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # получить текущие дату и время

    for j in DEVICES_RUN.index:
        try:

            HOSTNAME = DEVICES_RUN[HOSTNAME_COLUMN][j]
            PROFILE = DEVICES_RUN[PROFILE_COLUMN][j]
            IP_DEV = DEVICES_RUN[IP_COLUMN][j]

            print('{:<40s}{:<16s}{:<40s}{:<15s}{:<30d}'.format(HOSTNAME, IP_DEV, PROFILE, 'Analysing host', j))

            mac_config = rffile('{path}\\{filename}.list'.format(path=maclist_dir, filename=IP_DEV + '_mac'))

            if mac_config == None:
                mac_error = 'no mac file found'
                with open('{path}\\{filename}.macerr'.format(path=macerr_dir, filename=IP_DEV), 'w',
                          1) as file:  # открыть файл базы данных конфигов
                    file.write(mac_error)  # записать в файл
                continue

            # таблица MAC в формате pandas dataframe
            maclist_pd = list_parse(mac_config, MACLIST_PARSE_TEMPLATE[PROFILE])

            if maclist_pd.empty == True:
                mac_error = 'no data found'
                with open('{path}\\{filename}.macerr'.format(path=macerr_dir, filename=IP_DEV), 'w',
                          1) as file:  # открыть файл базы данных конфигов
                    file.write(mac_error)  # записать в файл
                continue
            elif maclist_pd.empty == False:
                mac_error = 'no error'
                with open('{path}\\{filename}.macerr'.format(path=macerr_dir, filename=IP_DEV), 'w',
                          1) as file:  # открыть файл базы данных конфигов
                    file.write(mac_error)  # записать в файл

            # нормализация MAC адресов и интерфейсов
            for i, mac in enumerate(maclist_pd['MAC']):
                maclist_pd.loc[i, 'MAC'] = mac_norm(mac)
                maclist_pd.loc[i, IF_COLUMN] = if_norm(maclist_pd.loc[i, IF_COLUMN])
            '''    
            DIFF_IF_LIST = maclist_pd['IF'].drop_duplicates() #список различных интерфейсов в таблице
            
            for IF in DIFF_IF_LIST:
                DATA_ON_IF_PD = maclist_pd[maclist_pd['IF'] == IF] 
                DIFF_MAC_ON_IF_PD = DATA_ON_IF_PD.drop_duplicates(subset=['MAC']) #dataframe MAC адресов на порту без повторений
                MAC_ON_IF_СOUNT = DATA_ON_IF_PD['MAC'].count() #количество MAC адресов на интерфейсе IF 

                maclist_pd.loc[,IF] 

            #обработка метрик (количество разных адресов на интерфейсе и проч)    
            for index,mac in enumerate(maclist_pd['MAC']):
                IF = maclist_pd.loc[index,'IF']

                DATA_ON_IF_PD = maclist_pd[maclist_pd['IF'] == IF] 
                DIFF_MAC_ON_IF_PD = DATA_ON_IF_PD.drop_duplicates(subset=['MAC']) #dataframe MAC адресов на порту без повторений
                MAC_ON_IF_СOUNT = DATA_ON_IF_PD['MAC'].count() #количество MAC адресов на интерфейсе IF


                #print(IF_PD)
                #IF_LIST = pdcol2str_zpt(IF_PD['IF'].drop_duplicates()) #список интерфейсов с искомым MAC
                #print(IF_LIST)
                #IF_COUNT = maclist_pd[maclist_pd['IF'] == IF].drop_duplicates().count() #количество записей о таких интерфейсах в таблице (по сути является количеством vlan, на которых есть этот MAC)

                maclist_pd.loc[index, 'MAC_ON_IF_СOUNT'] = MAC_ON_IF_СOUNT

            maclist_pd[DATE_COLUMN] = NOWSTR
            '''

            # lowercase poligon interfaces
            if PROFILE == 'poligon':
                maclist_pd[IF_COLUMN] = maclist_pd[IF_COLUMN].str.lower()

            maclist_pd.to_excel('{path}\\{filename}.xlsx'.format(path=mactable_dir, filename=IP_DEV + '_mactable'),
                                encoding='cp1251', index=False)  # save to xlsx file





        except Exception as e:
            print('maclist_to_excel()', HOSTNAME, PROFILE, e)
            continue

    print('TIME: {} sec'.format(str(time.time() - tic)))


def compare_mac_pd_tables(DATA_TABLE, MACLIST_PD, IP_DEV, HOSTNAME, PROFILE, CFG):
    try:
        DATA_TABLE = pdnan2none(DATA_TABLE)
        MACLIST_PD = pdnan2none(MACLIST_PD)
        # print(DATA_TABLE.head())
        # print(MACLIST_PD.head())

        OUT_PD = DATA_TABLE
        NOWSTR = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # получить текущие дату и время

        for index in DATA_TABLE.index:
            IF_PD = MACLIST_PD[MACLIST_PD['MAC'] == DATA_TABLE['MAC'][
                index]]  # список интерфейсов с искомым MAC адресом в формате pandas dataframe

            if not len(IF_PD): continue  # если список интерфейсов пустой - пропустить итерацию

            IF = list(IF_PD['IF'])[0]  # имя интерфейса
            # print(IF)
            IF_COUNT = IF_PD[
                'IF'].count()  # количество записей о таких интерфейсах в таблице (по сути является количеством vlan, на которых есть этот MAC)
            IF_LIST = pdcol2str_zpt(IF_PD['IF'].drop_duplicates())  # список интерфейсов с искомым MAC

            DATA_ON_IF_PD = MACLIST_PD[MACLIST_PD[
                                           'IF'] == IF]  # список MAC адресов и другие данныена текущем интерфейсе IF в формате dataframe
            DIFF_MAC_ON_IF_PD = DATA_ON_IF_PD.drop_duplicates(
                subset=['MAC'])  # dataframe MAC адресов на порту без повторений
            MAC_ON_IF_LIST = pdcol2str_zpt(DIFF_MAC_ON_IF_PD['MAC'])  # список MAC в формате строки без дубликатов

            VLANS_ON_IF_LIST = pdcol2str_zpt(
                DATA_ON_IF_PD['VLAN'].drop_duplicates())  # список VLAN на этом интерфейсе без дубликатов

            MAC_ON_IF_СOUNT = DATA_ON_IF_PD['MAC'].count()  # количество MAC адресов на интерфейсе IF
            # print(MAC_ON_IF_СOUNT)
            DMAC_ON_IF_СOUNT = DIFF_MAC_ON_IF_PD['MAC'].count()  # количество различных MAC на IF
            VLANS_ON_IF_СOUNT = DATA_ON_IF_PD['VLAN'].count()  # количество VLAN на интерфейсе IF
            # print('5')
            mac_num = 4  # предел мак адресов для записи

            if MAC_ON_IF_СOUNT > mac_num:
                MAC_ON_IF_LIST = 'More than ' + str(mac_num)

            VLAN = pdcol2str_zpt(IF_PD['VLAN'])  # VLAN текущего интерфейса
            MAC_TYPE = pdcol2str_zpt(IF_PD['TYPE'])  # тип изученного MAC

            # print('6')
            # обработка приоритетов по записи в файл

            if DATA_TABLE['MAC_ON_IF_СOUNT'][index] != None:  # если в ячейке что-то было записано ранее
                if MAC_ON_IF_СOUNT > DATA_TABLE['MAC_ON_IF_СOUNT'][
                    index]:  # если количество мак адресов на интерфейсе больше, чем то, что было найдено ранее
                    continue  # то пропустить цикл
            # print('7')
            # Нормализовать название интерфейса GE|Gi >> GigabitEthernet
            IF = if_norm(IF)
            # print('8')
            # print(IF)

            OUT_PD.loc[index, 'IF'] = IF
            OUT_PD.loc[index, 'IF_LIST'] = IF_LIST
            OUT_PD.loc[index, 'IF_COUNT'] = IF_COUNT
            OUT_PD.loc[index, 'VLAN_CUR'] = VLAN
            # print('9')
            if OUT_PD.loc[index, 'VLAN_ORIG'] != OUT_PD.loc[index, 'VLAN_ORIG'] or OUT_PD.loc[
                index, 'VLAN_ORIG'] == None:  # если поле VLAN_ORIG было пустым (Nan или None)
                OUT_PD.loc[
                    index, 'VLAN_ORIG'] = VLAN  # то записать первоначальный VLAN, к которому принадлежал МАК ## VLAN BACKUP
            # print('10')
            OUT_PD.loc[index, 'VLANS_ON_IF_LIST'] = VLANS_ON_IF_LIST
            OUT_PD.loc[index, 'MAC_ON_IF_LIST'] = MAC_ON_IF_LIST
            OUT_PD.loc[index, 'MAC_ON_IF_СOUNT'] = MAC_ON_IF_СOUNT
            OUT_PD.loc[index, 'DMAC_ON_IF_СOUNT'] = DMAC_ON_IF_СOUNT
            OUT_PD['DIFF_MAC_INDEX'] = OUT_PD['MAC_ON_IF_СOUNT'] - OUT_PD['DMAC_ON_IF_СOUNT']
            OUT_PD.loc[index, IP_COLUMN] = IP_DEV
            OUT_PD.loc[index, 'HOSTNAME'] = HOSTNAME
            OUT_PD.loc[index, 'MAC_TYPE'] = MAC_TYPE
            OUT_PD.loc[index, 'DATE'] = NOWSTR
            OUT_PD.loc[index, PROFILE_COLUMN] = PROFILE

            # получить конфиг на интерфейсе

            IF_INFO = if_info(IF, CFG, PROFILE)

            OUT_PD.loc[index, 'IF_CFG'] = IF_INFO['CFG']
            OUT_PD.loc[index, 'IF_TYPE'] = IF_INFO['TYPE']
            OUT_PD.loc[index, 'IF_Description'] = IF_INFO['Description']




    except Exception as e:
        print('compare_mac_pd_tables(DATA_TABLE, MACLIST_PD, IP_DEV, HOSTNAME, PROFILE, CFG)', e)
    return OUT_PD


def compare_mac_pd_tables_simple(DATA_TABLE, MACLIST_PD, IP_DEV, HOSTNAME, PROFILE):
    try:
        DATA_TABLE = pdnan2none(DATA_TABLE)
        MACLIST_PD = pdnan2none(MACLIST_PD)

        OUT_PD = DATA_TABLE
        NOWSTR = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # получить текущие дату и время

        for index in DATA_TABLE.index:
            if DATA_TABLE['MAC'][index] == None or DATA_TABLE['MAC'][index] == 'none':
                continue

            IF_PD = MACLIST_PD[MACLIST_PD['MAC'] == DATA_TABLE['MAC'][
                index]]  # список интерфейсов с искомым MAC адресом в формате pandas dataframe

            if not len(IF_PD): continue  # если список интерфейсов пустой - пропустить итерацию

            IF = list(IF_PD['IF'])[0]  # имя интерфейса
            # print(IF)
            IF_COUNT = IF_PD[
                'IF'].count()  # количество записей о таких интерфейсах в таблице (по сути является количеством vlan, на которых есть этот MAC)
            IF_LIST = pdcol2str_zpt(IF_PD['IF'].drop_duplicates())  # список интерфейсов с искомым MAC

            DATA_ON_IF_PD = MACLIST_PD[MACLIST_PD[
                                           'IF'] == IF]  # список MAC адресов и другие данныена текущем интерфейсе IF в формате dataframe
            DIFF_MAC_ON_IF_PD = DATA_ON_IF_PD.drop_duplicates(
                subset=['MAC'])  # dataframe MAC адресов на порту без повторений
            MAC_ON_IF_LIST = pdcol2str_zpt(DIFF_MAC_ON_IF_PD['MAC'])  # список MAC в формате строки без дубликатов

            VLANS_ON_IF_LIST = pdcol2str_zpt(
                DATA_ON_IF_PD['VLAN'].drop_duplicates())  # список VLAN на этом интерфейсе без дубликатов

            MAC_ON_IF_СOUNT = DATA_ON_IF_PD['MAC'].count()  # количество MAC адресов на интерфейсе IF
            # print(MAC_ON_IF_СOUNT)
            DMAC_ON_IF_СOUNT = DIFF_MAC_ON_IF_PD['MAC'].count()  # количество различных MAC на IF
            VLANS_ON_IF_СOUNT = DATA_ON_IF_PD['VLAN'].count()  # количество VLAN на интерфейсе IF
            # print('5')
            mac_num = 4  # предел мак адресов для записи

            if MAC_ON_IF_СOUNT > mac_num:
                MAC_ON_IF_LIST = 'More than ' + str(mac_num)

            VLAN = pdcol2str_zpt(IF_PD['VLAN'])  # VLAN текущего интерфейса
            MAC_TYPE = pdcol2str_zpt(IF_PD['TYPE'])  # тип изученного MAC

            # print('6')
            # обработка приоритетов по записи в файл

            if DATA_TABLE['MAC_ON_IF_СOUNT'][index] != None:  # если в ячейке что-то было записано ранее
                if MAC_ON_IF_СOUNT > DATA_TABLE['MAC_ON_IF_СOUNT'][
                    index]:  # если количество мак адресов на интерфейсе больше, чем то, что было найдено ранее
                    continue  # то пропустить цикл
            # print('7')
            # Нормализовать название интерфейса GE|Gi >> GigabitEthernet
            IF = if_norm(IF)
            # print('8')
            # print(IF)

            OUT_PD.loc[index, 'IF'] = IF
            # OUT_PD.loc[index, 'IF_LIST'] = IF_LIST
            # OUT_PD.loc[index, 'IF_COUNT'] = IF_COUNT
            OUT_PD.loc[index, 'VLAN_CUR'] = VLAN
            # print('9')
            # if OUT_PD.loc[index, 'VLAN_ORIG'] != OUT_PD.loc[index, 'VLAN_ORIG'] or OUT_PD.loc[index, 'VLAN_ORIG'] == None: #если поле VLAN_ORIG было пустым (Nan или None)
            #    OUT_PD.loc[index, 'VLAN_ORIG'] = VLAN #то записать первоначальный VLAN, к которому принадлежал МАК ## VLAN BACKUP
            # print('10')
            OUT_PD.loc[index, 'VLANS_ON_IF_LIST'] = VLANS_ON_IF_LIST
            OUT_PD.loc[index, 'MAC_ON_IF_LIST'] = MAC_ON_IF_LIST
            OUT_PD.loc[index, 'MAC_ON_IF_СOUNT'] = MAC_ON_IF_СOUNT
            OUT_PD.loc[index, 'DMAC_ON_IF_СOUNT'] = DMAC_ON_IF_СOUNT
            # OUT_PD['DIFF_MAC_INDEX'] = OUT_PD['MAC_ON_IF_СOUNT'] - OUT_PD['DMAC_ON_IF_СOUNT']
            OUT_PD.loc[index, IP_COLUMN] = IP_DEV
            OUT_PD.loc[index, 'HOSTNAME'] = HOSTNAME
            OUT_PD.loc[index, 'MAC_TYPE'] = MAC_TYPE
            # OUT_PD.loc[index, 'DATE'] = NOWSTR
            OUT_PD.loc[index, PROFILE_COLUMN] = PROFILE



    except Exception as e:
        print('compare_mac_pd_tables(DATA_TABLE, MACLIST_PD, IP_DEV, HOSTNAME, PROFILE, CFG)', e)
    return OUT_PD


# найти все вхождения по регулярному выражению в строке
def find_rx_sl(rx_str, str):  # одну первую найденную строку
    try:
        rx = re.compile(rx_str)

        data_list = rx.findall(str)

        if len(data_list) != 0:
            data_0 = data_list[0]
        else:
            data_0 = None

    except Exception as e:
        print('find_rx_sl()', str, e)
        return None

    return data_0


def find_rx_ml(rx_str, str, delimeter):  # все вхождения, с разделителем delimeter
    try:
        rx = re.compile(rx_str)

        data_list = rx.findall(str)

        if len(data_list) != 0:
            data_n = delimeter.join(data_list)
        else:
            data_n = None

    except Exception as e:
        print('find_rx_ml()', e)
        return None

    return data_n


def parse_if_cfg(IF_CFG, PROFILE):
    try:

        rx_vrf = {
            'cisco_ios': '\s+vrf (?:forwarding|member)\s(.*)',
            'poligon': '\sip vrf forwarding\s(.*)',
            'huawei': '\n\sip binding vpn-instance\s(.*)',
            'hp_comware': '\n\sip binding vpn-instance\s(.*)',
            'cisco_asa': '\n\svrf forwarding\s(.*)'
        }

        rx_port_type = {
            'cisco_ios': '\n\s+switchport mode\s(access|trunk)',
            'poligon': '\n\sswitchport mode\s(access|trunk)',
            'huawei': '\n\sport link-type\s(access|trunk|hybrid)',
            'hp_comware': '\n\sport link-type\s(access|trunk|hybrid).*'
        }

        rx_access_vlan = {
            'cisco_ios': '\n\s+switchport (?:access|trunk native) vlan\s(\d+)',
            'poligon': '\n\sswitchport pvid\s(\d+)',
            'huawei': '\n\sport (?:default|hybrid pvid|trunk pvid) vlan\s(\d+)',
            'hp_comware': '\n\sport access vlan\s(\d+)',
            'cisco_asa': '\n\svlan\s(\d+)'
        }

        rx_voice_vlan = {
            'cisco_ios': '\n\s+switchport voice vlan\s(\d+)',
            'poligon': '\n\sswitchport voice-vlan\s(\d+)',
            'huawei': '\n\svoice-vlan\s(\d+).*',
            'hp_comware': '\n\svoice-vlan\s(\d+).*'
        }

        rx_trunk_vlan = {
            'cisco_ios': '\n\s+switchport trunk allowed vlan\s(.*)',
            'poligon': '\n\sswitchport trunk vlan-allowed\s(.*)',
            'huawei': '\n\sport trunk allow-pass vlan\s(.*)',
            'hp_comware': '\n\sport trunk permit vlan\s(.*)'
        }

        rx_trunk_native_vlan = {
            'cisco_ios': '\n\s+switchport trunk native vlan\s(\d+)',
            'poligon': '\n\sswitchport pvid\s(\d+)',
            'huawei': '\n\sport \w+ pvid vlan (\d+)',
            'hp_comware': '\n\sport \w+ pvid vlan (\d+)'
        }

        rx_ip_helper = {
            'cisco_ios': '\n\s+ip helper-address.*\s(\d+\.\d+\.\d+\.\d+)',
            'poligon': '\n\sip helper-address.*\s(\d+\.\d+\.\d+\.\d+)',
            'huawei': '\n\sdhcp relay server-select\s(.*)',
            'hp_comware': '\n\sdhcp relay server-select\s(.*)',
            'cisco_asa': '\n\sdhcprelay server\s(\d+\.\d+\.\d+\.\d+)'
        }

        if PROFILE == 'cisco_asa':
            IF_INFO = {
                'IF': find_rx_ml('^interface\s(.*)(?:\n|$)', IF_CFG, ', '),
                'CFG': IF_CFG,
                'nameif': find_rx_ml('\n\snameif\s(.*)', IF_CFG, ', '),
                'IP': find_rx_ml('\n\s+ip address\s(\d+\.\d+\.\d+\.\d+)', IF_CFG, ', '),
                'MASK': find_rx_ml('\n\s+ip address\s(?:\d+\.\d+\.\d+\.\d+)\s(\d+\.\d+\.\d+\.\d+)', IF_CFG, ', '),
                'NETWORK': '',
                'Description': find_rx_ml('\n\sdescription\s(.*)', IF_CFG, ', '),
                'Shutdown': find_rx_ml('\n\s(shutdown)', IF_CFG, ', '),
                'IP Helper': find_rx_ml(rx_ip_helper[PROFILE], IF_CFG, ', ')
            }
        else:
            IF_INFO = {
                'CFG': IF_CFG,
                'IF': find_rx_ml('^interface\s(.*)(?:\n|$)', IF_CFG, ', '),
                'IP': find_rx_ml('\n\s+ip address\s(\d+\.\d+\.\d+\.\d+)', IF_CFG, ', '),
                'MASK': find_rx_ml('\n\s+ip address\s(?:\d+\.\d+\.\d+\.\d+)(?:\s|\/)(\d+\.\d+\.\d+\.\d+|\d{1,2})',
                                   IF_CFG, ', '),
                'NETWORK': '',
                'Description': find_rx_ml('\n\s+description\s(.*)', IF_CFG, ', '),
                'Shutdown': find_rx_ml('\n\s+(shutdown)', IF_CFG, ', '),

                'IP Helper': find_rx_ml(rx_ip_helper[PROFILE], IF_CFG, ', '),
                'VRF': find_rx_ml(rx_vrf[PROFILE], IF_CFG, ', '),
                'TYPE': find_rx_ml(rx_port_type[PROFILE], IF_CFG, ', '),
                'Access VLAN': find_rx_ml(rx_access_vlan[PROFILE], IF_CFG, ', '),
                'Voice VLAN': find_rx_ml(rx_voice_vlan[PROFILE], IF_CFG, ', '),
                'Trunk allowed vlans': find_rx_ml(rx_trunk_vlan[PROFILE], IF_CFG, ', '),

                'MAX_MAC': find_rx_ml(
                    '\n\s+(?:port-security max-mac-count|port-security max-mac-num|switchport port-security maximum)\s(\d+)\n',
                    IF_CFG, ', '),
                # spanning-tree
                'STP': find_rx_ml('\n\s+(?:stp|spanning-tree)\s(.*)', IF_CFG, '\n'),

                # storm control
                'storm-control': find_rx_ml('\n\s+storm-con\w+ (.*)', IF_CFG, '\n'),
                # 'storm-control broadcast':find_rx_ml('\n\sstorm-control broadcast (.*)',IF_CFG,','),
                # 'storm-control multicast':find_rx_ml('\n\sstorm-control multicast (.*)',IF_CFG,','),
                # 'storm-control unicast':find_rx_ml('\n\sstorm-control unicast (.*)',IF_CFG,','),
                # 'storm-control action':find_rx_ml('\n\sstorm-control action (.*)',IF_CFG,','),

            }

        return IF_INFO

    except Exception as e:
        print('parse_if_cfg()', e)
        return None


def if_info(IF, CFG, PROFILE):
    try:
        IF_CFG_PD = parse_cfg(CFG, '^interface {}$'.format(IF))

        if not IF_CFG_PD.empty:  # если конфиг найден, те не пустой
            IF_CFG = IF_CFG_PD['interface {}'.format(IF)]['cfg']

        IF_INFO = parse_if_cfg(IF_CFG, PROFILE)

        return IF_INFO

    except Exception as e:
        print('if_info(IF,CFG)', e)
        return None


def get_if(DEVICES_RUN):
    for CSV in DEVICES_RUN[DATA_COLUMN].drop_duplicates():
        try:
            # print('1')
            DEVICES_RUN_CSV = DEVICES_RUN[DEVICES_RUN[DATA_COLUMN] == CSV]

            DATA_TABLE = pd.read_excel('{path}\\{filename}'.format(path=data_dir, filename=CSV),
                                       encoding='cp1251')  # прочитать таблицу соответствия USER >> MAC
            DATA_TABLE = pdnan2none(DATA_TABLE)  # замена NaN на None
            # print(DATA_TABLE.head())
            # print('2')
            COUNT = DEVICES_RUN[IP_COLUMN].count()

            for j in DEVICES_RUN_CSV[IP_COLUMN].index:
                try:
                    IP_DEV = DEVICES_RUN_CSV[IP_COLUMN][j]  # ip адрес девайса
                    PROFILE = DEVICES_RUN_CSV[PROFILE_COLUMN][j]  # профиль девайса
                    HOSTNAME = DEVICES_RUN_CSV[HOSTNAME_COLUMN][j]

                    if HOSTNAME == None or not (HOSTNAME == HOSTNAME):  # если HOSTNAME пустое или Nan
                        HOSTNAME = 'unknown'

                    print('{:<40s}{:<16s}{:<40s}{:<3d}'.format(HOSTNAME, IP_DEV, 'Analysing', j))
                    # clear()

                    # print('3')

                    # CFG = rffile('{path}\\{filename}.cfg'.format(path = cfgbase_dir, filename = IP_DEV))
                    # print('33')
                    # нормализация MAC адресов

                    for i, mac in enumerate(DATA_TABLE['MAC']):
                        DATA_TABLE.loc[i, 'MAC'] = mac_norm(str(mac))

                    # print('333')
                    # MACLIST_PD= getmacinfo(IP_DEV = IP_DEV,PROFILE = PROFILE)  # получить данные с коммутатора
                    MACLIST_PD = pd.read_excel(
                        '{path}\\{filename}.xlsx'.format(path=mactable_dir, filename=IP_DEV + '_mactable'),
                        encoding='cp1251', dtype={'VLAN': str})  # прочитать таблицу соответствия USER >> MAC
                    MACLIST_PD = pdnan2none(MACLIST_PD)
                    # print('4')

                    # DATA_TABLE = compare_mac_pd_tables(DATA_TABLE, MACLIST_PD, IP_DEV, HOSTNAME, PROFILE, CFG)
                    DATA_TABLE = compare_mac_pd_tables_simple(DATA_TABLE, MACLIST_PD, IP_DEV, HOSTNAME, PROFILE)

                    DATA_TABLE.to_excel('{path}\\{filename}'.format(path=data_dir, filename=CSV), encoding='cp1251',
                                        index=False)  # save to xlsx file
                except Exception as e:
                    print('get_if(DEVICES_RUN) 2nd cycle', e)
                    continue

            print('Fin')

        except Exception as e:
            print('get_if(DEVICES_RUN)', e)
            continue


def find_if(DEVICES_RUN, PARENTTXT):
    pd_columns = [EXEC_COLUMN, SEGMENT_COLUMN, HOSTNAME_COLUMN, IP_COLUMN, PROFILE_COLUMN, IF_COLUMN,
                  'Description', 'TYPE', 'Shutdown', 'IP', 'MASK', 'NETWORK', 'VRF', 'Access VLAN',
                  'Voice VLAN', 'Trunk allowed vlans', 'IP Helper', 'MAX_MAC', 'MAX_MAC_DATA', 'MAX_MAC_VOICE',
                  'MAC_ON_IF_COUNT', 'DIFF_MAC_ON_IF_COUNT', 'STP', 'storm-control',
                  CMDCFG_COLUMN, RESULT_COLUMN, 'CFG']
    IP_IF_PD = pd.DataFrame(columns=pd_columns)
    # index = 0

    NOWSTR = datetime.datetime.now().strftime(
        '%Y-%m-%d_%H-%M-%S')  # получить текущие дату и время для имени файла результатов

    for j in DEVICES_RUN[IP_COLUMN].index:
        try:
            IP_DEV = DEVICES_RUN[IP_COLUMN][j]  # ip адрес девайса
            PROFILE = DEVICES_RUN[PROFILE_COLUMN][j]  # профиль девайса
            HOSTNAME = DEVICES_RUN[HOSTNAME_COLUMN][j]
            SEGMENT = DEVICES_RUN[SEGMENT_COLUMN][j]

            CFG = rffile('{path}\\{filename}.cfg'.format(path=cfgbase_dir, filename=IP_DEV))  # прочитать CFG из файла
            # CFG = DEVICES_RUN[CONFIG_COLUMN][j] #прочитать config из таблицы
            IF_PD = parse_cfg(CFG, PARENTTXT)

            print('{:<40s}{:<16s}{:<40s}{:<3d}'.format(HOSTNAME, IP_DEV, 'Analysing', j))
            # clear()

            for IF in IF_PD.columns:

                # IF = IF[10:] #отрезать первые 10 символов для получения имени интерфейса, из 'interface gi1/0/2', например
                IF_INFO = {'Segment': SEGMENT,
                           'Hostname': HOSTNAME,
                           'IP_DEV': IP_DEV,
                           'PROFILE': PROFILE}

                IF_INFO.update(parse_if_cfg(IF_PD[IF].cfg, PROFILE))

                if (IF_INFO['IP'] != None) and (IF_INFO['MASK'] != None):  # not blank
                    try:
                        IF_INFO['NETWORK'] = ipaddress.IPv4Interface(
                            IF_INFO['IP'] + '/' + IF_INFO['MASK']).network  # расчет подсети
                        # IF_INFO['NETWORK'] = 'not calculated'
                    except Exception as err:
                        IF_INFO['NETWORK'] = 'Error' + str(err)
                        pass

                IP_IF_PD = IP_IF_PD.append(IF_INFO, ignore_index=True)


        except Exception as e:
            print('find_ip_if(DEVICES_RUN)', HOSTNAME, IP_DEV, e)
            continue

    return IP_IF_PD


# добавить информацию по мак адресам на интерфейсах
def update_if_info(DEVICES_RUN_IF):
    for IP_DEV in DEVICES_RUN_IF[IP_COLUMN].drop_duplicates():
        try:
            PD_RUN = DEVICES_RUN_IF[DEVICES_RUN_IF[IP_COLUMN] == IP_DEV]

            PROFILE = PD_RUN[PROFILE_COLUMN].iloc[0]  # профиль девайса
            HOSTNAME = PD_RUN[HOSTNAME_COLUMN].iloc[0]  # hostname

            MACF_PATH = '{path}\{filename}.xlsx'.format(path=mactable_dir, filename=IP_DEV + '_mactable')

            if os.path.exists(MACF_PATH):  # если есть файл с данными о MAC адресах
                print('{:<40s}{:<16s}{:<60s}{:<15s}'.format(HOSTNAME, IP_DEV, MACF_PATH, 'Processing'))

                MACLIST_PD = pd.read_excel(MACF_PATH, encoding='cp1251',
                                           dtype={'VLAN': str, 'MAC': str})  # прочитать таблицу MAC адресов

                # MACLIST_PD = pd.read_excel(MACF_PATH, encoding='cp1251',dtype=str)  # прочитать таблицу MAC адресов

                MACLIST_PD = pdnan2none(MACLIST_PD)

                for i in PD_RUN.index:
                    try:
                        IF = PD_RUN[IF_COLUMN][i]
                        MACLIST_PD_RUN = MACLIST_PD[MACLIST_PD['IF'] == IF]
                        MAC_COUNT = MACLIST_PD_RUN['MAC'].count()  # количество MAC адресов на интерфейсе

                        DEVICES_RUN_IF.loc[i, 'MAC_ON_IF_COUNT'] = str(MAC_COUNT)

                        MACLIST_PD_RUN_DDMAC = MACLIST_PD_RUN.drop_duplicates(subset='MAC')
                        DIFF_MAC_COUNT = MACLIST_PD_RUN_DDMAC[
                            'MAC'].count()  # количество разных MAC адресов на интерфейсе

                        DEVICES_RUN_IF.loc[i, 'DIFF_MAC_ON_IF_COUNT'] = str(DIFF_MAC_COUNT)

                    except Exception as ee:
                        print('update_if_info(DEVICES_RUN_IF) 2nd cycle', HOSTNAME, IP_DEV, IF, ee)
                        continue

        except Exception as e:
            print('update_if_info(DEVICES_RUN_IF)', HOSTNAME, IP_DEV, e)
            continue
    print('Fin')
    return DEVICES_RUN_IF


def cmd_mac_sec_3548(DEVICES_RUN_IF):  # form portsec commands for Cisco 3548 models
    for IP_DEV in DEVICES_RUN_IF[IP_COLUMN].drop_duplicates():
        try:
            PD_RUN = DEVICES_RUN_IF[DEVICES_RUN_IF[IP_COLUMN] == IP_DEV]

            PROFILE = PD_RUN[PROFILE_COLUMN].iloc[0]  # профиль девайса
            HOSTNAME = PD_RUN[HOSTNAME_COLUMN].iloc[0]  # hostname

            MACF_PATH = '{path}\{filename}.xlsx'.format(path=mactable_dir, filename=IP_DEV + '_mactable')

            if os.path.exists(MACF_PATH):  # если есть файл с данными о MAC адресах
                print('{:<40s}{:<16s}{:<60s}{:<15s}'.format(HOSTNAME, IP_DEV, MACF_PATH, 'Processing'))

                MACLIST_PD = pd.read_excel(MACF_PATH, encoding='cp1251',
                                           dtype={'VLAN': str, 'MAC': str})  # прочитать таблицу MAC адресов

                # MACLIST_PD = pd.read_excel(MACF_PATH, encoding='cp1251',dtype=str)  # прочитать таблицу MAC адресов

                MACLIST_PD = pdnan2none(MACLIST_PD)

                for i in PD_RUN.index:
                    try:
                        IF = PD_RUN[IF_COLUMN][i]
                        MACLIST_PD_RUN = MACLIST_PD[MACLIST_PD['IF'] == IF]
                        MAC_COUNT = MACLIST_PD_RUN['MAC'].count()  # количество MAC адресов на интерфейсе

                        DEVICES_RUN_IF.loc[i, 'MAC_ON_IF_COUNT'] = str(MAC_COUNT)

                        MACLIST_PD_RUN_DDMAC = MACLIST_PD_RUN.drop_duplicates(subset='MAC')
                        DIFF_MAC_COUNT = MACLIST_PD_RUN_DDMAC[
                            'MAC'].count()  # количество разных MAC адресов на интерфейсе

                        DEVICES_RUN_IF.loc[i, 'DIFF_MAC_ON_IF_COUNT'] = str(DIFF_MAC_COUNT)

                        if not MACLIST_PD_RUN.empty:
                            for j in MACLIST_PD_RUN.index:
                                DEVICES_RUN_IF.loc[i, 'CMDCFG'] = str(DEVICES_RUN_IF.loc[i, 'CMDCFG'])
                                if DEVICES_RUN_IF.loc[i, 'CMDCFG'] == 'nan':
                                    DEVICES_RUN_IF.loc[i, 'CMDCFG'] = ''
                                mac = MACLIST_PD_RUN.loc[j, 'MAC']
                                mac_vlan = MACLIST_PD_RUN.loc[j, 'VLAN']
                                DEVICES_RUN_IF.loc[
                                    i, 'CMDCFG'] += 'mac-address-table secure {}.{}.{} {} vlan {}\n'.format(mac[0:4],
                                                                                                            mac[4:8],
                                                                                                            mac[8:12],
                                                                                                            IF,
                                                                                                            mac_vlan)

                    except Exception as ee:
                        print('update_if_info3548(DEVICES_RUN_IF) 2nd cycle', HOSTNAME, IP_DEV, IF, ee)
                        continue
        except Exception as e:
            print('update_if_info3548(DEVICES_RUN_IF)', HOSTNAME, IP_DEV, e)
            continue
    print('Fin')
    return DEVICES_RUN_IF


def find_strange_ports(DEVICES_RUN, THRESHOLD_MIN, THRESHOLD_MAX, VLAN_COUNT_THRESHOLD):
    tic = time.time()  # начало выполнения

    IF_NUM = 0
    MAC_NUM = 0
    STRANGE_IF_PD = pd.DataFrame()
    index = 0

    NOWSTR = datetime.datetime.now().strftime(
        '%Y-%m-%d_%H-%M-%S')  # получить текущие дату и время для имени файла результатов

    os.mkdir("{path}\\{nowtime}".format(path=strangeports_dir, nowtime=NOWSTR))  # создать каталог

    for j in DEVICES_RUN[IP_COLUMN].index:
        try:
            IP_DEV = DEVICES_RUN[IP_COLUMN][j]  # ip адрес девайса
            PROFILE = DEVICES_RUN[PROFILE_COLUMN][j]  # профиль девайса
            HOSTNAME = DEVICES_RUN[HOSTNAME_COLUMN][j]
            SEGMENT = DEVICES_RUN[SEGMENT_COLUMN][j]

            print(SEGMENT, IP_DEV, HOSTNAME, PROFILE)

            MACLIST_PD = pd.read_excel(
                '{path}\{filename}.xlsx'.format(path=mactable_dir, filename=IP_DEV + '_mactable'), encoding='cp1251',
                dtype={'VLAN': str, 'MAC': str})  # прочитать таблицу MAC адресов

            MACLIST_PD = pdnan2none(MACLIST_PD)

            MACLIST_PD_OUT = pd.DataFrame(columns=MACLIST_PD.columns)

            for IF in MACLIST_PD['IF'].drop_duplicates():

                if re.match(r'cpu|po.*|eth.*tru.*|vl.*', IF.lower()) is None:  # Check trunk and etherchannel interfaces

                    MACLIST_PD_RUN = MACLIST_PD[MACLIST_PD['IF'] == IF]
                    MACLIST_PD_RUN_DDVLAN = MACLIST_PD_RUN['VLAN'].drop_duplicates()
                    DIFF_VLAN_COUNT = MACLIST_PD_RUN_DDVLAN.count()

                    if DIFF_VLAN_COUNT <= VLAN_COUNT_THRESHOLD:

                        MACLIST_PD_RUN_DDMAC = MACLIST_PD_RUN.drop_duplicates(subset='MAC')
                        DIFF_MAC_COUNT = MACLIST_PD_RUN_DDMAC['MAC'].count()

                        if THRESHOLD_MIN <= DIFF_MAC_COUNT <= THRESHOLD_MAX:

                            # MACLIST_PD_OUT = MACLIST_PD_OUT.append(MACLIST_PD_RUN)

                            STRANGE_IF_PD.loc[index, SEGMENT_COLUMN] = SEGMENT
                            STRANGE_IF_PD.loc[index, HOSTNAME_COLUMN] = HOSTNAME
                            STRANGE_IF_PD.loc[index, IP_COLUMN] = IP_DEV
                            STRANGE_IF_PD.loc[index, PROFILE_COLUMN] = PROFILE

                            IF_full = if_norm(IF)  # нормализация IF
                            STRANGE_IF_PD.loc[index, IF_COLUMN] = IF_full

                            # получить информацию об интерфейсе из конфига устройства
                            CFG = rffile('{path}\\{filename}.cfg'.format(path=cfgbase_dir, filename=IP_DEV))

                            IF_INFO = if_info(IF_full, CFG, PROFILE)

                            if IF_INFO is not None:
                                STRANGE_IF_PD.loc[index, 'Type'] = IF_INFO['TYPE']
                                STRANGE_IF_PD.loc[index, 'Description'] = IF_INFO['Description']
                                STRANGE_IF_PD.loc[index, 'Config'] = IF_INFO['CFG']
                                STRANGE_IF_PD.loc[index, 'Port-sec max MAC'] = IF_INFO['MAX_MAC']

                            # информация о MAC адресах и VLAN

                            STRANGE_IF_PD.loc[index, 'Diff_VLAN_Count'] = str(DIFF_VLAN_COUNT)
                            STRANGE_IF_PD.loc[index, 'VLAN'] = pdcol2str_zpt(MACLIST_PD_RUN_DDVLAN)

                            STRANGE_IF_PD.loc[index, 'Diff_MAC_Count'] = str(DIFF_MAC_COUNT)
                            STRANGE_IF_PD.loc[index, 'MAC'] = pdcol2str_zpt(MACLIST_PD_RUN_DDMAC['MAC'])
                            '''
                            mac_num=4 #предел мак адресов для записи
                            if DIFF_MAC_COUNT > mac_num: 
                                STRANGE_IF_PD.loc[index,'MAC'] = 'More than ' + str(mac_num)
                            else:
                                STRANGE_IF_PD.loc[index,'MAC'] = pdcol2str_zpt(MACLIST_PD_RUN_DDMAC['MAC'])
                            '''

                            index += 1

                            IF_NUM += MACLIST_PD_RUN['IF'].drop_duplicates().count()
                            MAC_NUM += MACLIST_PD_RUN['MAC'].drop_duplicates().count()

            '''
            if not MACLIST_PD_OUT.empty:
                MACLIST_PD_OUT.to_csv('csv\\mactable\\strangeports\\{0}\\{1}_mactable_strangeports.csv'.format(NOWSTR,HOSTNAME), sep=';', encoding='cp1251',index=False)  # save to csv file
            '''
        except Exception as e:
            print('find_strange_ports(DEVICES_RUN, THRESHOLD_MIN, THRESHOLD_MAX, VLAN_COUNT_THRESHOLD)', HOSTNAME,
                  IP_DEV)
            # print(IF,IF_full)
            print(e)
            continue

    vremya = str(time.time() - tic) + ' sec'

    with open('{path}\\{nowtime}\\Information.txt'.format(path=strangeports_dir, nowtime=NOWSTR), 'w',
              1) as file:  # открыть файл базы данных конфигов
        file.write('DATE: {0}\n'.format(NOWSTR))  # записать в файл

        file.write('\nINPUT PARAMS:\n')
        file.write('-----------------------------\n')
        file.write('THRESHOLD NUMBER OF VLANS ON IF: {0}\n'.format(VLAN_COUNT_THRESHOLD))  # записать в файл
        file.write('THRESHOLD MIN MAC ON IF: {0}\n'.format(THRESHOLD_MIN))  # записать в файл
        file.write('THRESHOLD MAX MAC ON IF: {0}\n'.format(THRESHOLD_MAX))  # записать в файл
        file.write(
            '\nКритерий поиска - выбираются интерфейсы, на которых присутствует не более {vlan} VLAN, с количеством различных(!) MAC адресов на интерфейсе от {mac_min} до {mac_max}\n\n'.format(
                vlan=VLAN_COUNT_THRESHOLD, mac_min=THRESHOLD_MIN, mac_max=THRESHOLD_MAX))

        file.write('-----------------------------\n')
        '''
        file.write('\nRESULTS:\n')
        file.write('-----------------------------\n')
        file.write('Strange interfaces total: {0}\n'.format(IF_NUM))  # записать в файл
        file.write('MAC total: {0}\n'.format(MAC_NUM))  # записать в файл
        file.write('\nПримерная оценка количества необходимых портов: {0}\n'.format(MAC_NUM - IF_NUM))
        file.write('-----------------------------\n')
        '''
        file.write('TIME: {}'.format(vremya))

    print('Strange interfaces total: ', IF_NUM)
    print('MAC total: ', MAC_NUM)
    print('Примерная оценка количества необходимых портов: {0}'.format(MAC_NUM - IF_NUM))
    print(vremya)

    if not STRANGE_IF_PD.empty:
        STRANGE_IF_PD.to_excel(
            '{path}\\{nowtime}\\Strangeports_Summary_{nowtime}.xlsx'.format(path=strangeports_dir, nowtime=NOWSTR),
            encoding='cp1251', index=False)  # save to xslx file

    return STRANGE_IF_PD


def form_cmd(VAR, PROF, TEMPLATE):  # формирование команды на отправку
    try:
        env = Environment(loader=FileSystemLoader('templates'), trim_blocks=True)  # templates directory
        template_ios = env.get_template(TEMPLATE)  # шаблон команды
        CMDCFG = template_ios.render(DICT=VAR, PROFILE=PROF)  # подстановка значений переменных в шаблон
        # print(CMDCFG)
        # print(filter(None,CMDCFG.splitlines()))
        # CMDCFG = list(filter(None, CMDCFG.splitlines()))  # split config lines && delete empty ones
        CMDCFG = re.sub(r'\s*(.*)\s*?(\n)', r'\1\n', CMDCFG)
    except Exception as e:
        print('form_cmd(VAR, PROF, TEMPLATE)', e)

    return CMDCFG


def form_cmd_pd(DEVICES_RUN, FILENAME):
    for i in DEVICES_RUN.index:
        HOSTNAME = DEVICES_RUN[HOSTNAME_COLUMN][i]
        IP = DEVICES_RUN[IP_COLUMN][i]
        PROFILE = DEVICES_RUN[PROFILE_COLUMN][i]
        CMD_TEMPLATE = DEVICES_RUN[CMD_TEMPLATE_COLUMN][i]

        try:
            # если поле DATAFILE - не пустое, данные для формирования команды берутся из xlsx файла
            if DEVICES_RUN[DATA_COLUMN][i] == DEVICES_RUN[DATA_COLUMN][i] and DEVICES_RUN[DATA_COLUMN][i] != None:
                # Обработка данных
                DATA_TABLE = pd.read_excel(
                    '{path}\\{filename}'.format(path=data_dir, filename=DEVICES_RUN[DATA_COLUMN][i]), encoding='cp1251',
                    dtype={'MAX_MAC': str, 'VLAN_NEW': str, 'VLAN_CUR': str, 'VLAN_ORIG': str, 'Access VLAN': str,
                           'Voice VLAN': str})  # прочитать таблицу с данными
                # DATA_TABLE = pd.read_excel('{path}\\{filename}'.format(path = data_dir, filename = DEVICES_RUN[DATA_COLUMN][i]), encoding='cp1251', dtype=str )  # прочитать таблицу с данными

                DATA_TABLE_DEV = DATA_TABLE[
                    DATA_TABLE[IP_COLUMN] == IP]  # выбираем только те строки, где IP совпадает с текущим девайсом
                DATA_TABLE_DEV_RUN = DATA_TABLE_DEV[
                    DATA_TABLE_DEV['Exec'].notnull()]  # только те строки, что помечены для перехода
                # print(DATA_TABLE_DEV_RUN)
                if DATA_TABLE_DEV_RUN.empty:  # if cmdcfg is empty
                    continue
                ### PARSE CONFIG && FORM CMD
                for j in DATA_TABLE_DEV_RUN.index:
                    CMDCFG = form_cmd(DATA_TABLE_DEV_RUN.loc[j], PROFILE, CMD_TEMPLATE)
                    DATA_TABLE.loc[j, CMDCFG_COLUMN] = CMDCFG
                    # print(CMDCFG)
                # write to excel
                print('Saving file ' + '{path}\\{filename}'.format(path=data_dir, filename=DEVICES_RUN[DATA_COLUMN][i]))
                # DATA_TABLE = pdnan2none(DATA_TABLE)
                DATA_TABLE.to_excel('{path}\\{filename}'.format(path=data_dir, filename=DEVICES_RUN[DATA_COLUMN][i]),
                                    encoding='cp1251', index=False)

            # если поле DATAFILE - пустое, команда формируется для девайса
            else:
                DEVICES_RUN.loc[i, CMDCFG_COLUMN] = form_cmd(DEVICES_RUN.loc[i], PROFILE, CMD_TEMPLATE)
                print('Saving file ' + '{path}\\{filename}.xlsx'.format(path=data_dir, filename=FILENAME))
                DEVICES_RUN.to_excel('{path}\\{filename}.xlsx'.format(path=data_dir, filename=FILENAME),
                                     encoding='cp1251', index=False)  # save to ls file
        except Exception as e:
            print('form_cmd_pd(DEVICES_RUN)', e)
            pass

    print('Command render completed')
    return DEVICES_RUN


# отправка команды на девайс

def sendcmd(**kwargs):
    HOSTNAME = kwargs[HOSTNAME_COLUMN]
    tic = time.time()  # начало выполнения
    IP = kwargs[IP_COLUMN]

    CON_PROFILE = kwargs[PROFILE_COLUMN]  # default ssh
    if kwargs[CONMODE_COLUMN] == 'telnet':
        CON_PROFILE = kwargs[PROFILE_COLUMN] + '_telnet'

    # env = Environment(loader=FileSystemLoader('templates'),trim_blocks=True) #templates directory

    if IP != IP or kwargs[PROFILE_COLUMN] != kwargs[PROFILE_COLUMN] or (kwargs[EXEC_COLUMN] != 'X' and kwargs[
        EXEC_COLUMN] != 'D'): return kwargs  # NaN check, проверка пустых строк + проверка строк для выполнения - ничего с ними не делать, пропускать

    DEVICE_PARAMS = {'device_type': CON_PROFILE,
                     'ip': IP,
                     'username': USER if kwargs[USER_COLUMN] != kwargs[USER_COLUMN] else kwargs[USER_COLUMN],
                     'password': PASSWORD if kwargs[PASSWORD_COLUMN] != kwargs[PASSWORD_COLUMN] else kwargs[
                         PASSWORD_COLUMN],
                     'secret': PASSWORD if kwargs[SECRET_COLUMN] != kwargs[SECRET_COLUMN] else kwargs[SECRET_COLUMN]}
    # 'global_delay_factor':1}

    NOWSTR = datetime.datetime.now().strftime(
        '_%Y-%m-%d_%H-%M-%S')  # получить текущие дату и время для имени файла результатов

    try:

        if kwargs[EXEC_COLUMN] == 'X':  # если режим отправки команд включен

            with ConnectHandler(**DEVICE_PARAMS) as ssh:

                if HOSTNAME == None or not (HOSTNAME == HOSTNAME):  # если HOSTNAME пустое или Nan
                    HOSTNAME = 'unknown'

                print('{:<40s}{:<16s}{:<40s}{:<15s}'.format(HOSTNAME, IP, 'Connection to device', 'Processing'))
                HOSTNAME = ssh.find_prompt()
                HOSTNAME = reg.sub('', HOSTNAME)  # нормализация hostname

                # проверка несоответствия имени хоста в файле и девайсе
                hostname_mismatch(kwargs[HOSTNAME_COLUMN], HOSTNAME)

                ###BACKUP

                print('{:<40s}{:<16s}{:<40s}{:<15s}'.format(HOSTNAME, IP, 'Backing up', 'Processing'))

                kwargs[CONFIG_COLUMN] = ssh.send_command(
                    SHRUN_CMD[kwargs[PROFILE_COLUMN]])  # получить конфиг отправить команду show run

                with open('{path}\\{filename}.cfg'.format(path=cfgbase_dir, filename=IP, nowtime=NOWSTR), 'w',
                          encoding='utf-8') as file:  # открыть файл
                    file.write(kwargs[CONFIG_COLUMN])  # записать в файл

                with open('{path}\\{filename}_{nowtime}.cfg'.format(path=cfg_dir, filename=HOSTNAME, nowtime=NOWSTR),
                          'w', encoding='utf-8') as file:  # открыть файл
                    file.write(kwargs[CONFIG_COLUMN])  # записать в файл

                print(color.GREEN + '{:<40s}{:<16s}{:<40s}{:<15s}'.format(HOSTNAME, IP, 'Backup', 'OK') + color.END)

                # если поле команды не пустое
                if (kwargs[CMDCFG_COLUMN] != None) and (kwargs[CMDCFG_COLUMN] == kwargs[CMDCFG_COLUMN]) and (
                        kwargs[CMDCFG_COLUMN] != 'nan'):
                    CMDCFG = kwargs[CMDCFG_COLUMN]
                    try:
                        ###SEND CMD
                        print('{:<40s}{:<16s}{:<40s}{:<15s}'.format(HOSTNAME, IP, 'Send global commands to device',
                                                                    'Processing'))
                        with open('{path}\\{filename}_{nowtime}.result'.format(path=result_dir, filename=HOSTNAME,
                                                                               nowtime=NOWSTR), 'w',
                                  1) as file:  # открыть фstrайл

                            file.write(HOSTNAME + '\n' + IP + '\n')
                            file.write('\n##CONFIG COMMANDS########\n' + CMDCFG + '\n#########################\n')
                            file.write('\n##RESULT#################\n')
                            # ssh.enable()
                            RESULT = ssh.send_config_set(CMDCFG, exit_config_mode=False)
                            file.write(RESULT)

                            file.write('\nSaving Configuration\n')
                            file.write(ssh.send_config_set(SAVECFG_CMD[kwargs[PROFILE_COLUMN]], exit_config_mode=False))
                            file.write('\nDisconnecting\n')
                            # ssh.disconnect()

                            file.write('\n#########################\n')  # записать в файл
                            file.write('\n##TIME#################\n' + str(
                                time.time() - tic) + ' sec\n#########################\n')  # записать в файл

                            print(color.GREEN + '{:<40s}{:<16s}{:<40s}{:<15s}'.format(HOSTNAME, IP,
                                                                                      'Send global commands to device',
                                                                                      'OK') + color.END)

                        with open('{path}\\{filename}.result'.format(path=cfgbase_dir, filename=IP), 'w',
                                  1) as file:  # открыть фstrайл
                            file.write(RESULT)

                    except Exception as e_cmd:
                        print(color.RED + '{:<40s}{:<16s}{:<40s}{:<15s}{:<25s}'.format(HOSTNAME, IP, 'Send CMD to DEV',
                                                                                       'FAIL', str(e_cmd) + color.END))
                        with open('{path}\\{filename}.result'.format(path=cfgbase_dir, filename=IP), 'w', 1) as file:
                            file.write(str(e_cmd))

                # если поле DATAFILE - не пустое, данные для формирования команды берутся из xlsx файла
                if (kwargs[DATA_COLUMN] == kwargs[DATA_COLUMN]) and (kwargs[DATA_COLUMN] != None):

                    # Обработка данных       
                    DATA_TABLE = pd.read_excel('{path}\\{filename}'.format(path=data_dir, filename=kwargs[DATA_COLUMN]),
                                               encoding='cp1251',
                                               dtype={'VLAN_NEW': str, 'VLAN_CUR': str, 'VLAN_ORIG': str,
                                                      'Access VLAN': str,
                                                      'Voice VLAN': str})  # прочитать таблицу с данными
                    DATA_TABLE_DEV = DATA_TABLE[
                        DATA_TABLE[IP_COLUMN] == IP]  # выбираем только те строки, где IP совпадает с текущим девайсом
                    DATA_TABLE_DEV_RUN = DATA_TABLE_DEV[
                        DATA_TABLE_DEV['Exec'].notnull()]  # только те строки, что помечены для перехода

                    if DATA_TABLE_DEV_RUN.empty:  # exit function if cmdcfg is empty
                        return kwargs

                    ###SEND CMD
                    print('{:<40s}{:<16s}{:<40s}{:<15s}'.format(HOSTNAME, IP, 'Send CMD to IF', 'Processing'))
                    try:
                        with open('{path}\\{filename}_IF_{nowtime}.result'.format(path=result_dir, filename=HOSTNAME,
                                                                                  nowtime=NOWSTR), 'w',
                                  1) as file:  # открыть фstrайл
                            file.write(HOSTNAME + '\n' + IP + '\n')
                            # file.write('\n##CONFIG COMMANDS########\n' + norm(reg_blank_lines,CMDCFG) + '\n#########################\n')
                            file.write('\n##RESULT#################\n')

                            for i in DATA_TABLE_DEV_RUN.index:
                                try:
                                    CMDCFG = DATA_TABLE_DEV_RUN.loc[i, CMDCFG_COLUMN]
                                    if CMDCFG == CMDCFG and CMDCFG != None:
                                        # CMDCFG_norm = norm(reg_spaces,CMDCFG) #исключить все пробельные символы \s
                                        CMDCFG_norm2 = norm(reg_blank_lines, CMDCFG)  # исключить пустые строки
                                        print(
                                            '{:<40s}{:<16s}{:<40s}{:<15s}{:<25s}'.format(HOSTNAME, IP, 'Send CMD to IF',
                                                                                         'Processing',
                                                                                         DATA_TABLE_DEV_RUN.loc[
                                                                                             i, IF_COLUMN]))
                                        CMD_RESULT = ssh.send_config_set(CMDCFG_norm2, exit_config_mode=False)
                                        DATA_TABLE_DEV_RUN.loc[i, RESULT_COLUMN] = CMD_RESULT
                                        file.write('\n' + CMD_RESULT + '\n')
                                except Exception as e_cmd:
                                    # DATA_TABLE_DEV_RUN.loc[i,RESULT_COLUMN] = str(e_cmd)
                                    print(color.RED + '{:<40s}{:<16s}{:<40s}{:<15s}{:<25s}'.format(HOSTNAME, IP,
                                                                                                   'Send CMD to IF',
                                                                                                   'FAIL',
                                                                                                   DATA_TABLE_DEV_RUN.loc[
                                                                                                       i, IF_COLUMN]) + color.END)
                                    continue

                            DATA_TABLE_DEV_RUN.to_excel(
                                '{path}\\{filename}_if_result.xlsx'.format(path=cfgbase_dir, filename=IP),
                                encoding='cp1251', index=False)

                            file.write('\nSaving Configuration\n')
                            file.write(ssh.send_config_set(SAVECFG_CMD[kwargs[PROFILE_COLUMN]], exit_config_mode=False))
                            file.write('\nDisconnecting\n')
                            # ssh.disconnect()

                            file.write('\n#########################\n')  # записать в файл
                            file.write('\n##TIME#################\n' + str(
                                time.time() - tic) + ' sec\n#########################\n')  # записать в файл

                            print(color.GREEN + '{:<40s}{:<16s}{:<40s}{:<15s}'.format(HOSTNAME, IP,
                                                                                      'Send command to interfaces',
                                                                                      'OK') + color.END)

                        ssh.disconnect()
                    except Exception as ee:
                        DATA_TABLE_DEV_RUN.to_excel(
                            '{path}\\{filename}_if_result.xlsx'.format(path=cfgbase_dir, filename=IP),
                            encoding='cp1251', index=False)
                        print('\nError occured with', IP)
                        print('sendcmd(**kwargs)', ee)
                        ssh.disconnect()

    except Exception as e:
        # print('\nError occured with', IP)
        # print('sendcmd(**kwargs)',e)
        print(color.RED + '{:<40s}{:<16s}{:<100s}'.format(HOSTNAME, IP, 'sendcmd() error' + str(e)) + color.END)

        if kwargs[CONMODE_COLUMN] != 'telnet' and ("timed-out" in str(e)) and ("WinError" in str(e)):

            print(color.YELLOW + '{:<40s}{:<16s}{:<40s}'.format(HOSTNAME, IP, 'Trying telnet connection') + color.END)
            kwargs[CONMODE_COLUMN] = 'telnet'

            with open('{path}\\{filename}.conmode'.format(path=cfgbase_dir, filename=IP), 'w',
                      1) as file:  # открыть файл базы данных конфигов
                file.write('telnet')  # записать в файл

            sendcmd(**kwargs)


        else:
            # with open('{path}\\{filename}.err'.format(path = cfgbase_dir, filename = kwargs[IP_COLUMN]), 'w',1) as file:  # открыть файл базы данных конфигов
            kwargs[RESULT_COLUMN] = str(e)
            with open('{path}\\{filename}_{nowtime}.result'.format(path=result_dir, filename=HOSTNAME, nowtime=NOWSTR),
                      'a', 1) as file:  # открыть файл базы данных конфигов
                file.write(str(HOSTNAME) + '\n' + str(IP) + '\n' + str(CON_PROFILE) + '\n' + str(e))  # записать в файл

    return kwargs


# параллельное выполнение команд, потоки

def conn_threads(cmd, index, step, **devices):
    if not yes_or_no('Batch operation. Do you want to continue?'):
        return False

    devices = pd.DataFrame(data=devices)
    count = devices[index].count()
    threads = []

    anykey = 'go_step'

    j = 0

    while j <= count:

        devices_batch = devices[j:j + step]

        # print(devices_batch)
        print('Batch index {:<20d}'.format(j))

        for i, IP in enumerate(devices_batch[index]):
            th = threading.Thread(target=cmd, kwargs=(devices_batch.iloc[i]))
            th.start()
            threads.append(th)

        for th in threads:
            th.join()

        if anykey == 'go_step':
            anykey = press_any_key_or_quit()
            if anykey == 'quit':
                break

        j = j + step;

        # clear() #очистка экрана jupyter

    print('Fin')


def inventory_parser(DEVICES_OUT):
    try:
        for index in DEVICES_OUT.index:

            # cisco_ios parse
            if DEVICES_OUT['PROFILE'][index] == 'cisco_ios' and DEVICES_OUT['VER'][index] == DEVICES_OUT['VER'][index]:
                if DEVICES_OUT['INV'][index] == DEVICES_OUT['INV'][index]:
                    template = open('{path}\\cisco_ios_serial_all.txtfsm'.format(path=txtfsm_dir))
                    fsm = textfsm.TextFSM(template)
                    result = fsm.ParseText(DEVICES_OUT['INV'][index])

                    result_str = ''
                    for row in result:
                        result_str = result_str + ' '.join(row) + '\n'

                    DEVICES_OUT.loc[index, 'SN'] = result_str

                # парсер модели устройства
                rx_model = '\s+(WS-C.*?|C\d+-.?-.*?|\scisco\sNexus.*?)\s'
                model = find_rx_sl(rx_model, DEVICES_OUT['VER'][index])

                if model != None:
                    DEVICES_OUT.loc[index, 'Model'] = model

                # парсер system image устройства
                rx_image = 'System image file is \".*?\:\/?(.*)\"'
                image = find_rx_sl(rx_image, DEVICES_OUT['VER'][index])

                if image != None:
                    DEVICES_OUT.loc[index, 'Image'] = norm(reg_image, image)

            # poligon parser
            elif DEVICES_OUT['PROFILE'][index] == 'poligon' and DEVICES_OUT['VER'][index] == DEVICES_OUT['VER'][index]:
                # парсер модели устройства
                rx_model = '(Arlan\s.*)\n'
                model = find_rx_sl(rx_model, DEVICES_OUT['VER'][index])

                if model != None:
                    DEVICES_OUT.loc[index, 'Model'] = model


            # hp_comware parser
            elif DEVICES_OUT['PROFILE'][index] == 'hp_comware' and DEVICES_OUT['VER'][index] == DEVICES_OUT['VER'][
                index]:
                # парсер модели устройства
                rx_model = '\s+(HP\sA?\d+)\s'
                model = find_rx_sl(rx_model, DEVICES_OUT['VER'][index])

                if model != None:
                    DEVICES_OUT.loc[index, 'Model'] = model

            # huawei parser
            elif DEVICES_OUT['PROFILE'][index] == 'huawei' and DEVICES_OUT['INV'][index] == DEVICES_OUT['INV'][index]:
                template = open('{path}\\huawei_serial.txtfsm'.format(path=txtfsm_dir))
                fsm = textfsm.TextFSM(template)
                result = fsm.ParseText(DEVICES_OUT['INV'][index])

                result_str = ''
                for row in result:
                    result_str = result_str + ''.join(row) + ' '

                DEVICES_OUT.loc[index, 'SN'] = result_str

                # парсер модели устройства
                rx_model = '\n(?:HUAWEI|Quidway)\s(\w+\d+.*?)\s'
                model = find_rx_sl(rx_model, DEVICES_OUT['VER'][index])

                if model != None:
                    DEVICES_OUT.loc[index, 'Model'] = model

            # cisco_asa parser
            elif DEVICES_OUT['PROFILE'][index] == 'cisco_asa' and DEVICES_OUT['INV'][index] == DEVICES_OUT['INV'][
                index]:
                template = open('{path}\\cisco_asa_serial.txtfsm'.format(path=txtfsm_dir))
                fsm = textfsm.TextFSM(template)
                result = fsm.ParseText(DEVICES_OUT['INV'][index])

                result_str = ''
                for row in result:
                    result_str = result_str + ''.join(row) + ' '

                DEVICES_OUT.loc[index, 'SN'] = result_str

                # парсер модели устройства
                rx_model = 'Hardware:\s+(.*)\n'

                model = find_rx_sl(rx_model, DEVICES_OUT['VER'][index])

                if model != None:
                    DEVICES_OUT.loc[index, 'Model'] = model
    except Exception as e:
        print('inventory_parser(DEVICES_OUT)', e)

    return DEVICES_OUT
