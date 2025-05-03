sudo apt update
sudo apt install -y python3 python3-pip build-essential libssl-dev libffi-dev
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install nodejs -y
pip3 install websockets psutil --break-system-packages
sudo npm install react react-dom react-chartjs-2 chart.js chartjs-plugin-annotation react-icons
sudo npm install -g serve 
mv controlAPI.py ../.controlAPI.py
