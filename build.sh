
cd python
python setup.py bdist_egg
cd ../scala
sbt clean package
