/*******************************************************************************
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package org.eclipse.rdf4j.sail.base;

import org.eclipse.rdf4j.common.iteration.CloseableExceptionConvertingIteration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.DistinctIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.RDFStarTripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.sail.SailException;

/**
 * Implementation of the TripleSource interface using {@link SailDataset}
 */
class SailDatasetTripleSource implements TripleSource, RDFStarTripleSource {

	private final ValueFactory vf;

	private final SailDataset dataset;

	public SailDatasetTripleSource(ValueFactory vf, SailDataset dataset) {
		this.vf = vf;
		this.dataset = dataset;
	}

	@Override
	public String toString() {
		return dataset.toString();
	}

	@Override
	public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource subj, IRI pred,
			Value obj, Resource... contexts) throws QueryEvaluationException {
		try {
			CloseableIteration<? extends Statement, SailException> statements = dataset.getStatements(subj, pred, obj,
					contexts);
			if (statements instanceof EmptyIteration) {
				return EMPTY_ITERATION;
			}
			return new Eval(statements);
		} catch (SailException e) {
			throw new QueryEvaluationException(e);
		}
	}

	@Override
	public ValueFactory getValueFactory() {
		return vf;
	}

	public static class Eval extends
			CloseableExceptionConvertingIteration<Statement, QueryEvaluationException, CloseableIteration<? extends Statement, SailException>> {
		public Eval(CloseableIteration<? extends Statement, SailException> iter) {
			super(iter, QueryEvaluationException::new);
		}
	}

	static class TriplesIteration extends
			CloseableExceptionConvertingIteration<Triple, QueryEvaluationException, CloseableIteration<? extends Triple, ? extends Exception>> {

		public TriplesIteration(CloseableIteration<? extends Triple, ? extends Exception> iter) {
			super(iter, QueryEvaluationException::new);
		}

	}

	@Override
	public CloseableIteration<? extends Triple, QueryEvaluationException> getRdfStarTriples(Resource subj, IRI pred,
			Value obj) throws QueryEvaluationException {
		try {
			// In contrast to statement retrieval (which gets de-duplicated later on when handling things like
			// projections and conversions) we need to make sure we de-duplicate the RDF-star triples here.
			CloseableIteration<? extends Triple, SailException> triples = dataset.getTriples(subj, pred, obj);
			if (triples instanceof EmptyIteration)
				return EMPTY_TRIPLE_ITERATION;
			return new DistinctIteration<>(new TriplesIteration(triples));
		} catch (SailException e) {
			throw new QueryEvaluationException(e);
		}
	}
}
