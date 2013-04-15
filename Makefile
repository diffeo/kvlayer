
clean:
	rm -rf build dist src/*.egg-info/

.PHONY : build
build: clean
	python setup.py build

build_eggs: build
	python setup.py bdist_egg

build_packages: build_eggs
	python setup.py bdist_rpm

install: build
	## I think this `clean --all` step removes existing versions
	## from site-packages that would conflict when we install
	python setup.py clean --all
	python setup.py install

test:
	cd src && python ../runtests.py tests -s	

