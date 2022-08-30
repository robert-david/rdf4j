/*******************************************************************************
 * Copyright (c) 2021 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package org.eclipse.rdf4j.sail.lmdb;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.sail.lmdb.model.LmdbValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 *
 */
class LmdbEvaluationStatistics extends EvaluationStatistics {

	private static final Logger logger = LoggerFactory.getLogger(LmdbEvaluationStatistics.class);

	private final ValueStore valueStore;

	private final TripleStore tripleStore;

	private static final Cache<Value, Object> VALUE_DOES_NOT_EXIST_CACHE = CacheBuilder.newBuilder()
			.maximumSize(100000)
			.expireAfterWrite(1, TimeUnit.SECONDS)
			.concurrencyLevel(Runtime.getRuntime().availableProcessors() * 2)
			.build();
	private static final Cache<StatementPattern, Double> STATEMENT_CARDINALITY_CACHE = CacheBuilder.newBuilder()
			.maximumSize(100000)
			.expireAfterWrite(1, TimeUnit.SECONDS)
			.concurrencyLevel(Runtime.getRuntime().availableProcessors() * 2)
			.build();

	public LmdbEvaluationStatistics(ValueStore valueStore, TripleStore tripleStore) {
		this.valueStore = valueStore;
		this.tripleStore = tripleStore;
	}

	@Override
	protected CardinalityCalculator createCardinalityCalculator() {
		return new LmdbCardinalityCalculator();
	}

	protected class LmdbCardinalityCalculator extends CardinalityCalculator {

		@Override
		protected double getCardinality(StatementPattern sp) {
			try {
				Value subj = resourceOrNull(getConstantValue(sp.getSubjectVar()));
				Value pred = iriOrNull(getConstantValue(sp.getPredicateVar()));
				Value obj = getConstantValue(sp.getObjectVar());
				Value context = resourceOrNull(getConstantValue(sp.getContextVar()));
				if (logger.isTraceEnabled()) {
					if (STATEMENT_CARDINALITY_CACHE.getIfPresent(sp) != null) {
						// double space after "hit" makes log lines align better
						logger.trace("Cache hit  for in STATEMENT_CARDINALITY_CACHE for {}", sp);
					} else {
						logger.trace("Cache miss for in STATEMENT_CARDINALITY_CACHE for {}", sp);
					}
				}

				return STATEMENT_CARDINALITY_CACHE.get(sp,
						() -> cardinality((Resource) subj, (IRI) pred, obj, (Resource) context));
			} catch (ExecutionException e) {
				logger.error("Failed to estimate statement pattern cardinality, falling back to generic implementation",
						e);
				return super.getCardinality(sp);
			}
		}

		protected Value getConstantValue(Var var) {
			return (var != null) ? var.getValue() : null;
		}
	}

	private Value resourceOrNull(Value value) {
		// can happen when a previous optimizer has inlined a comparison operator.
		// this can cause, for example, the subject variable to be equated to a literal value.
		// See SES-970
		if (value == null || !value.isResource()) {
			return null;
		}
		return value;
	}

	private Value iriOrNull(Value value) {
		// can happen when a previous optimizer has inlined a comparison operator. See SES-970
		if (value == null || !value.isIRI()) {
			return null;
		}
		return value;
	}

	private double cardinality(Resource subj, IRI pred, Value obj, Resource context) throws IOException {
		if (subj != null && VALUE_DOES_NOT_EXIST_CACHE.getIfPresent(subj) != null) {
			logger.trace("Cache hit  in VALUE_DOES_NOT_EXIST_CACHE for {}", subj);
			return 0;
		} else if (pred != null && VALUE_DOES_NOT_EXIST_CACHE.getIfPresent(pred) != null) {
			logger.trace("Cache hit  in VALUE_DOES_NOT_EXIST_CACHE for {}", pred);
			return 0;
		} else if (obj != null && VALUE_DOES_NOT_EXIST_CACHE.getIfPresent(obj) != null) {
			logger.trace("Cache hit  in VALUE_DOES_NOT_EXIST_CACHE for {}", obj);
			return 0;
		} else if (context != null && VALUE_DOES_NOT_EXIST_CACHE.getIfPresent(context) != null) {
			logger.trace("Cache hit  in VALUE_DOES_NOT_EXIST_CACHE for {}", context);
			return 0;
		}

		long subjID = LmdbValue.UNKNOWN_ID;
		if (subj != null) {
			subjID = valueStore.getId(subj);
			if (subjID == LmdbValue.UNKNOWN_ID) {
				logger.trace("Cache miss  in VALUE_DOES_NOT_EXIST_CACHE for {}", subj);
				VALUE_DOES_NOT_EXIST_CACHE.put(subj, 0);
				return 0;
			}
		}

		long predID = LmdbValue.UNKNOWN_ID;
		if (pred != null) {
			predID = valueStore.getId(pred);
			if (predID == LmdbValue.UNKNOWN_ID) {
				logger.trace("Cache miss  in VALUE_DOES_NOT_EXIST_CACHE for {}", pred);
				VALUE_DOES_NOT_EXIST_CACHE.put(pred, 0);
				return 0;
			}
		}

		long objID = LmdbValue.UNKNOWN_ID;
		if (obj != null) {
			objID = valueStore.getId(obj);
			if (objID == LmdbValue.UNKNOWN_ID) {
				logger.trace("Cache miss  in VALUE_DOES_NOT_EXIST_CACHE for {}", obj);
				VALUE_DOES_NOT_EXIST_CACHE.put(obj, 0);
				return 0;
			}
		}

		long contextID = LmdbValue.UNKNOWN_ID;
		if (context != null) {
			contextID = valueStore.getId(context);
			if (contextID == LmdbValue.UNKNOWN_ID) {
				logger.trace("Cache miss  in VALUE_DOES_NOT_EXIST_CACHE for {}", context);
				VALUE_DOES_NOT_EXIST_CACHE.put(context, 0);
				return 0;
			}
		}

		return tripleStore.cardinality(subjID, predID, objID, contextID);
	}
}
