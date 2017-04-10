#!/bin/sh
cd spark
./sbin/start-master.sh
./sbin/start-slave.sh spark://anoop-VirtualBox:7077
