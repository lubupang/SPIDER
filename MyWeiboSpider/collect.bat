cd %~dp0
pip install -r requirements.txt -i https://pypi.douban.com/simple/
python CollectMode.py config.json
pause