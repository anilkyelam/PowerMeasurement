#!/usr/local/bin/bash

ORDERING=( 40 38 44 42 34 36 30 32 )
USER="ayelam"

declare -A MACS=( [38]="ec:0d:9a:68:21:c4" [40]="ec:0d:9a:68:21:c8" [42]="ec:0d:9a:68:21:ac" [44]="ec:0d:9a:68:21:a4" [36]="ec:0d:9a:68:21:a0" [34]="ec:0d:9a:68:21:a8" [32]="ec:0d:9a:68:21:84" [30]="ec:0d:9a:68:21:b0" )
P=2

echo "#!/bin/bash" > ./arp_tables_ft.sh
echo "" >> ./arp_tables_ft.sh
echo "" > ./pingall.sh

for H in ${!ORDERING[@]}
do
    POD=$(($H/$P))
    AGG=$(($H%$P))
    echo "setting IP for b09-${ORDERING[H]}"
    # ssh -t $USER@b09-${ORDERING[H]}.sysnet.ucsd.edu sudo ifconfig enp101s0 "10.$POD.$AGG.1" netmask 255.0.0.0
    echo "sudo arp -s 10.$POD.$AGG.1 ${MACS[${ORDERING[H]}]}" >> ./arp_tables_ft.sh
    echo "ping -c 1 10.$POD.$AGG.1" >> ./pingall.sh
done

chmod +x ./arp_tables_ft.sh
scp ./arp_tables_ft.sh $USER@b09-40.sysnet.ucsd.edu:/home/$USER/bf-cluster/
scp ./pingall.sh $USER@b09-40.sysnet.ucsd.edu:/home/$USER/bf-cluster/
rm ./arp_tables_ft.sh
rm ./pingall.sh

sleep 1

for H in ${!ORDERING[@]}
do
    ssh -t $USER@b09-${ORDERING[H]}.sysnet.ucsd.edu bash /home/$USER/bf-cluster/arp_tables_ft.sh
done

for H in ${!ORDERING[@]}
do
    ssh -t $USER@b09-${ORDERING[H]}.sysnet.ucsd.edu bash /home/$USER/bf-cluster/pingall.sh
done
