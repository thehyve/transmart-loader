java -cp `find lib -name 'weld-logger-*.jar'`:`find . -name 'loader-*.jar'` \
	com.recomdata.pipeline.entrypoint.Bootstrap $@
