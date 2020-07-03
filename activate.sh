echo 'ok :)'
py -m virtualenv beam
source ./beam/Scripts/activate
python -m pip install --upgrade setuptools
python -m pip install -r requirements.txt
echo 'termino python'
docker run -d --name mongo-conekta -p 27017:27017 mongo
echo 'termino mongo'