#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify output file!'
    exit 1
fi
# write some really existing User Agents and one malformed
rm -rf input
mkdir input


UNVIERSITY=("MEPHI" "MADI" "SPBGU")
YEAR=("2019" "2020" "2021")
MONTH=("01" "02" "03" "04" "05" "06" "07" "08" "09" "10" "11" "12")

#"MEPHI;4;26-06-2020 10:11:28;0"

for i in {1..100}
	do
	  UNIVERCITY=${UNVIERSITY[$((RANDOM % ${#UNVIERSITY[*]}))]}
	  ID=$(($RANDOM % 10))
	  HOURSTART=$((10 + $(($RANDOM % 3))))
	  HOUREND=$(($(($RANDOM % 7)) + 1 + $HOURSTART))
	  YEAR=${YEAR[$((RANDOM % ${#YEAR[*]}))]}
	  DAY=$(($i % 10 + 10))
	  MONTH=${MONTH[$((RANDOM % ${#MONTH[*]}))]}

	  NUMBER=$(($NUMBER + $(($RANDOM % 5000))))
	  RESULTNUMBER=$(($STARTNUMBER+$NUMBER))
		RESULT1="$UNIVERCITY;$ID;$DAY-$MONTH-$YEAR $HOURSTART:20:20;0"
		RESULT2="$UNIVERCITY;$ID;$DAY-$MONTH-$YEAR $HOUREND:20:20;1"
		echo $RESULT1 >> input/$1.1
		echo $RESULT2 >> input/$1.1
	done

	for i in {1..100}
    	do
    	  UNIVERCITY=${UNVIERSITY[$((RANDOM % ${#UNVIERSITY[*]}))]}
    	  ID=$(($RANDOM % 10))
    	  YEAR=${YEAR[$((RANDOM % ${#YEAR[*]}))]}
    	  DAY=$(($i % 10 + 10))
    	  MONTH=${MONTH[$((RANDOM % ${#MONTH[*]}))]}

    		RESULT="$UNIVERCITY;$ID;NAME_OF_WORK;$DAY-$MONTH-$YEAR"
    		echo $RESULT >> input/$1.2
    	done

