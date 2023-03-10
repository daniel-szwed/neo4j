package pl.kielce.tu.psr.neo4j.ogm;

interface Service<T> {
	
    T read(Long id);

    Iterable<T> readAll();

    void delete(Long id);
    
    void deleteAll();

    void createOrUpdate(T object);
}