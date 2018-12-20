import time
#import usbtmc
import _thread
import vxi11
import os, sys

#https://stackoverflow.com/questions/8600161/executing-periodic-actions-in-python
def do_every(period,max_count,f,*args):
    def g_tick():
        t = time.time()
        count = 0
        while True:
            count += 1 #0.001
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
def print_device(period, max_count):
	count = 0
	print(period)
	print(max_count)
	while count < max_count:
		t = time.time()
		device=instr.ask("*IDN?")
		print('{:.4f}'.format(time.time())+"\n")
		time.sleep(period)
		count += 1

def device_name(outfile):
	device=instr.ask("*IDN?")
	outfile.write('{:.4f}'.format(time.time())+"\n")

timestamp = sys.argv[1]
max_count = int(sys.argv[2])
period = int(sys.argv[3])
outfile = open("power_"+timestamp+".txt", "w")
instr=vxi11.Instrument("172.19.222.92","gpib0,12")

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
instr.write("*IDN?")
do_every(period, max_count, power_reading, outfile)
#_thread.start_new_thread( print_device, (1, max_count) )
#do_every(0.01, max_count, power_reading, outfile)
outfile.close()
