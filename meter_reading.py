import time
#import usbtmc
import vxi11
import os, sys

#https://stackoverflow.com/questions/8600161/executing-periodic-actions-in-python
def do_every(period,max_count,f,*args):
    def g_tick():
        t = time.time()
        count = 0
        while True:
            count += 1
            yield max(t + count*period - time.time(),0)
    g = g_tick()
    count = 0 
    print('Start time {:.4f}'.format(time.time()))
    f(*args)
    while count < max_count:
        count=count+1
        time.sleep(next(g))
        f(*args)

def power_reading(outfile):
    #print('Loop time {:.4f}'.format(time.time()))
    reading=instr.ask("measure:scalar:power:real? 0")
    #print(reading)
    outfile.write('{:.4f},'.format(time.time())+reading+"\n")	
    #time.sleep(.1)

timestamp = sys.argv[1]
max_count = int(sys.argv[2])
outfile = open("power_"+timestamp+".txt", "w")
#instr=vxi11.Instrument("172.19.222.92")
instr=vxi11.Instrument("172.19.222.92","gpib0,12")
#instr = usbtmc.Instrument(usbtmc.list_devices()[0])

##Handle the USB coonection can have e
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
do_every(1, max_count, power_reading, outfile)
outfile.close()
