cd ./powerpanel
rm nohup.out
nohup node powerpanel.js profile1.json & 
echo "start powerpanel #1"
nohup node powerpanel.js profile2.json & 
echo "start powerpanel #2"
nohup node powerpanel.js profile3.json & 
echo "start powerpanel #3"
cd ../camera1
rm nohup.out
nohup python fakecamera.py &
echo "start camera #1" 
cd ../camera2
rm nohup.out
nohup python fakecamera.py & 
echo "start camera #2"

cd ../
pwd

