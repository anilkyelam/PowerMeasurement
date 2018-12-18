import time
import vxi11
# import usbtmc
import os, sys
import datetime


# https://stackoverflow.com/questions/8600161/executing-periodic-actions-in-python
def do_every(period, max_count, f, *args):
    def g_tick():
        t = time.time()
        count = 0
        while True:
            count += 1
            yield max(t + count*period - time.time(), 0)

    g = g_tick()
    count = 0
    print('Start time {:.4f}'.format(time.time()))
    f(*args)
    while count < max_count:
        count=count+1
        time.sleep(next(g))
        f(*args)

        # Flush every now and then
        if count%10 == 0:
            outfile.flush()


def power_reading():
    reading=instr.ask("measure:scalar:power:real? 0")
    # print('Reading: ' + datetime.datetime.now().time().isoformat()+ ", " + reading+"\n")
    outfile.write('{:.4f},'.format(time.time())+reading+"\n")


if sys.argv.__len__() < 2:
    print("Please provide destination folder")
    exit(-1)

folder_path = sys.argv[1]
max_count = 100000         # Setting it to expire after 1 day if we missed killing it.
outfile = open(os.path.join(folder_path, "power_readings.txt"), "w")

# instr=vxi11.Instrument("172.19.222.92")
instr=vxi11.Instrument("172.19.222.92","gpib0,12")
# instr = usbtmc.Instrument(usbtmc.list_devices()[0])


# Handle the USB connection can have
try:
    instr.ask("*IDN?")
except Exception as ex:
    print(ex)
    pass

try:
    instr.ask("measure:scalar:power:real? 0")
except Exception as ex:
    print(ex)
    pass

print(instr.ask("*IDN?"))
do_every(1, max_count, power_reading)
outfile.close()
