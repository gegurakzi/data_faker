package io.malachai.datafaker;

import io.malachai.datafaker.parser.ConfigParser;
import io.malachai.datafaker.parser.JSONConfigParser;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

public class EntityManagerLoader {

    public EntityManager load(Reader reader) throws IOException {
        EntityManager entityManager = new EntityManager();

        ConfigParser parser = new JSONConfigParser();

        List<Source> sources = parser.parse(reader);

        for (int s = sources.size() - 1; s >= 0; s--) {
            Source source = sources.get(s);
            entityManager.addSource(source);
        }
        return entityManager;
    }


}
