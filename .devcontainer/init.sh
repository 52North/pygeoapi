export VS_WS='52Npygeoapi'
echo "alias serve="/workspaces/${VS_WS}/start.sh"" >> /root/.bashrc
echo "alias setup='python3 /workspaces/${VS_WS}/setup.py install'" >> /root/.bashrc
echo "alias reinit="/workspaces/${VS_WS}/reinit.sh"" >> /root/.bashrc

chmod +x /workspaces/$VS_WS/start.sh
chmod +x /workspaces/$VS_WS/reinit.sh

source /root/.bashrc

pip install -e .
python3 setup.py install

cp pygeoapi-config.yml example-config.yml