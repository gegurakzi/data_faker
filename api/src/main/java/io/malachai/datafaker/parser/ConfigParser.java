package io.malachai.datafaker.parser;

import io.malachai.datafaker.Source;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

public interface ConfigParser {

    List<Source> parse(Reader reader) throws IOException;

}
