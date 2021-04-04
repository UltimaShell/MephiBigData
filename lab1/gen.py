from random import randrange
from datetime import timedelta
from datetime import datetime
import random
import time
import re

warnings = ['0', '1', '2', '3', '4', '5', '6', '7']
devs = ['pci', 'acpiphp', 'SELinux', 'smpboot', 'pcpu-alloc', 'ACPI', 'pci_bus', 'pnp']

start = input('input seed date(EXAMPLE MM/DD/YYYY 05:00:00): ')
if re.match("(?<!\d)(?:0?[1-9]|[12][0-9]|3[01])/(?:0?[1-9]|1[0-2])/(?:19[0-9][0-9]|20[0-9][0-9]) (0[0-9]|1[0-9]|2[0-3])(:(0[0-9]|[1-5][0-9])){2}", start) == None:
    print("BAD DATE")
    exit(1)

cou = input('input row count: ')
if cou.isdigit() == None:
    print("BAD DIGIT")
    exit(1)

d1 = datetime.strptime(start, '%m/%d/%Y %H:%M:%S')
d1 = d1.timestamp()
d2 = datetime.fromtimestamp(d1).strftime( '%b %d %Y %H:%M:%S')#to string

f = open("input/result", 'wb')
for j in range(int(cou)):
    war_res = str(random.choice(warnings))
    dev_res = str(random.choice(devs))
    time_res = d2
    d2 = time.mktime(datetime.strptime(d2, '%b %d %Y %H:%M:%S').timetuple())#to seconds
    d2=d2+random.randint(1,10)
    d2 = datetime.fromtimestamp(d2).strftime( '%b %d %Y %H:%M:%S')#to string
    result_str = time_res + ' ' + war_res + ' ' +'kernel:' + dev_res +'\n'
    f.write(str.encode(result_str))
f.close()
print("SUCCESS")
