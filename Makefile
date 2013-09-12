VERSION := $(shell php -r 'echo (new SimpleXMLElement(file_get_contents("pom.xml")))->version;')
JAR_FILE := target/loader-$(VERSION).jar

target/lib: pom.xml
	rm -rf $@
	mvn dependency:copy-dependencies -DoutputDirectory=$@

$(JAR_FILE): src pom.xml
	mvn package

transmart-loader.tar.xz: $(JAR_FILE) target/lib loader.sh
	tar -cJf $@ loader.sh -C target $(patsubst target/%,%,$(JAR_FILE) target/lib)
