package org.eclipse.rdf4j.http.server.repository.transaction;

import org.eclipse.rdf4j.common.io.FileUtil;
import org.eclipse.rdf4j.http.protocol.Protocol;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

/**
 * Test for bnodes uniqueness which checks whether by default blank nodes are added with unique identifiers to transactions
 * or their identifiers are preserved by specifying a parameter for server instructions when parsing request data.
 */
public class TestBnodesUniquenessInTransactions {

	private MockHttpServletRequest request;
	private MockHttpServletResponse response;

	private final String repositoryID = "test-repo";
	private File dataDir;
	private Repository repository;

	private final String query = "select * where { \n" +
			"\t?s ?p ?o .\n" +
			"}";

	@BeforeEach
	public void setUp() throws IOException {
		dataDir = FileUtil.createTempDir(repositoryID);

		repository = new SailRepository(new NativeStore(dataDir));
		repository.init();

		request = new MockHttpServletRequest();
		response = new MockHttpServletResponse();
	}

	@AfterEach
	public void tearDown() throws Exception {
		repository.shutDown();
		FileUtil.deleteDir(dataDir);
	}

	@Test
	public void shouldImportUniqueBnodes() throws Exception {
		executeTransactionAction("<urn:a> <urn:b> _:c .");
		executeTransactionAction("<urn:a> <urn:o> _:c .");

		try (RepositoryConnection connection = repository.getConnection()) {
			List<BindingSet> result = QueryResults.asList(connection.prepareTupleQuery(query).evaluate());

			Assertions.assertNotEquals(result.get(0).getValue("o").stringValue(),
					result.get(1).getValue("o").stringValue());
		}
	}

	@Test
	public void shouldImportUniqueBnodesWithRequestParam() throws Exception {
		request.setParameter(Protocol.PRESERVE_BNODE_ID_PARAM_NAME, "false");

		executeTransactionAction("<urn:a> <urn:b> _:c .");
		executeTransactionAction("<urn:a> <urn:o> _:c .");

		try (RepositoryConnection connection = repository.getConnection()) {
			List<BindingSet> result = QueryResults.asList(connection.prepareTupleQuery(query).evaluate());

			Assertions.assertNotEquals(result.get(0).getValue("o").stringValue(),
					result.get(1).getValue("o").stringValue());
		}
	}

	@Test
	public void shouldImportPreservedBnodes() throws Exception {
		request.setParameter(Protocol.PRESERVE_BNODE_ID_PARAM_NAME, "true");

		executeTransactionAction("<urn:a> <urn:b> _:node .");
		executeTransactionAction("<urn:a> <urn:o> _:node .");

		try (RepositoryConnection connection = repository.getConnection()) {
			List<BindingSet> result = QueryResults.asList(connection.prepareTupleQuery(query).evaluate());

			Assertions.assertEquals("node", result.get(0).getValue("o").stringValue());
			Assertions.assertEquals("node", result.get(1).getValue("o").stringValue());
		}
	}

	/**
	 * Start a new transaction and add data to it using the default settings of parser config
	 *
	 * @param data is the content of the request which represents the data to be added to the repository
	 */
	private void executeTransactionAction(String data) throws Exception {
		Transaction txn = new Transaction(repository);
		ActiveTransactionRegistry.INSTANCE.register(txn);

		final UUID transactionId = txn.getID();

		request.setRequestURI("/repositories/" + repositoryID + "/transactions/" + transactionId);
		request.setPathInfo(repositoryID + "/transactions/" + transactionId);
		request.setMethod(HttpMethod.PUT.name());
		request.setParameter(Protocol.ACTION_PARAM_NAME, "ADD");
		request.setContent(data.getBytes(StandardCharsets.UTF_8));
		request.setContentType(RDFFormat.TURTLE.getDefaultMIMEType());

		TransactionController transactionController = new TransactionController();

		response = new MockHttpServletResponse();
		transactionController.handleRequestInternal(request, response);

		txn.close();
		ActiveTransactionRegistry.INSTANCE.deregister(txn);
	}

}
