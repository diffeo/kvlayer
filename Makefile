.PHONY : clean build install test register check

ENVDIR:=tmp/accumulo-saltstack-env
CLUSTER_SIZE?=1

clean:
	python setup.py clean --all
	rm -rf build dist src/*.egg-info/

build: clean
	python setup.py thrift
	python setup.py build

install: build
	python setup.py install

test: 
	python setup.py test

register:
	python setup.py sdist bdist_egg upload

check:
	pylint -i y --output-format=parseable src/`git remote -v | grep origin | head -1 | cut -d':' -f 2 | cut -d'.' -f 1`

$(ENVDIR):
	sudo apt-get install python-m2crypto python-dev python-virtualenv \
		python-pip build-essential
	virtualenv --system-site-packages $(ENVDIR)
	. $(ENVDIR)/bin/activate && \
        pip install apache-libcloud && \
        pip install mako && \
        pip install salt && \
	pip install fabric && \
        pip install -e git+https://github.com/vhgroup/salt-cloud.git@v0.8.9-stable#egg=salt-cloud

cluster-clean:
	rm -fR $(ENVDIR)

cluster: $(ENVDIR)
	. $(ENVDIR)/bin/activate && \
	CLUSTER_SIZE=$(CLUSTER_SIZE) salt-cloud \
		-C cloud/cloud -m cloud/cloud.map \
		--providers-config=cloud/cloud.providers \
		--profiles=cloud/cloud.profiles -y \
		--out=yaml > tmp/salt-cloud.out && \
	python cloud/launch.py

cluster-destroy: $(ENVDIR)
	. $(ENVDIR)/bin/activate && \
	CLUSTER_SIZE=$(CLUSTER_SIZE) salt-cloud \
		-C cloud/cloud -m cloud/cloud.map \
		--providers-config=cloud/cloud.providers \
		--profiles=cloud/cloud.profiles -y -d

