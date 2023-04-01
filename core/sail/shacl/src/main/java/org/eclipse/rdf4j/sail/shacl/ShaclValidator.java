/*******************************************************************************
 * Copyright (c) 2023 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 ******************************************************************************/

package org.eclipse.rdf4j.sail.shacl;

import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.shacl.ast.ContextWithShapes;
import org.eclipse.rdf4j.sail.shacl.ast.Shape;
import org.eclipse.rdf4j.sail.shacl.ast.planNodes.SingleCloseablePlanNode;
import org.eclipse.rdf4j.sail.shacl.ast.planNodes.ValidationExecutionLogger;
import org.eclipse.rdf4j.sail.shacl.ast.planNodes.ValidationTuple;
import org.eclipse.rdf4j.sail.shacl.results.ValidationReport;
import org.eclipse.rdf4j.sail.shacl.results.lazy.LazyValidationReport;
import org.eclipse.rdf4j.sail.shacl.results.lazy.ValidationResultIterator;
import org.eclipse.rdf4j.sail.shacl.wrapper.data.ConnectionsGroup;
import org.eclipse.rdf4j.sail.shacl.wrapper.data.RdfsSubClassOfReasoner;
import org.eclipse.rdf4j.sail.shacl.wrapper.data.VerySimpleRdfsBackwardsChainingConnection;
import org.eclipse.rdf4j.sail.shacl.wrapper.shape.CombinedShapeSource;
import org.eclipse.rdf4j.sail.shacl.wrapper.shape.ShapeSource;

public class ShaclValidator {

	public static final Resource[] ALL_CONTEXTS = {};

	public static ValidationReport validate(SailRepository shapesRepo, SailRepository dataRepo) {

		List<ContextWithShapes> shapes;
		try (SailRepositoryConnection shapesConnection = shapesRepo.getConnection()) {
			shapesConnection.begin(IsolationLevels.NONE);
			try (ShapeSource shapeSource = new CombinedShapeSource(shapesConnection,
					shapesConnection.getSailConnection())) {
				shapes = Shape.Factory.getShapes(shapeSource.withContext(ALL_CONTEXTS),
						new Shape.ParseSettings(true, true));
			}
			shapesConnection.commit();
		}

		try (SailRepositoryConnection dataRepoConnection = dataRepo.getConnection()) {

			RdfsSubClassOfReasoner reasoner;

			try (SailRepositoryConnection shapesConnection = shapesRepo.getConnection()) {
				reasoner = RdfsSubClassOfReasoner.createReasoner(
						dataRepoConnection.getSailConnection(), shapesConnection.getSailConnection(),
						new ValidationSettings(ALL_CONTEXTS, false, true, false));
			}

			VerySimpleRdfsBackwardsChainingConnection verySimpleRdfsBackwardsChainingConnection = new VerySimpleRdfsBackwardsChainingConnection(
					dataRepoConnection.getSailConnection(), reasoner);

			return performValidation(shapes, new ConnectionsGroup(verySimpleRdfsBackwardsChainingConnection, null,
					null, null, new Stats(), () -> reasoner,
					new ShaclSailConnection.Settings(true, true, true, IsolationLevels.NONE), true));
		}

	}

	private static ValidationReport performValidation(List<ContextWithShapes> shapes,
			ConnectionsGroup connectionsGroup) {

		List<ValidationResultIterator> collect = shapes
				.stream()
				.flatMap(contextWithShapes -> contextWithShapes
						.getShapes()
						.stream()
						.map(shape -> shape.generatePlans(connectionsGroup,
								new ValidationSettings(contextWithShapes.getDataGraph(), false, true, false)))
				)
				.map(planNode -> {
					planNode = new SingleCloseablePlanNode(planNode, ValidationExecutionLogger.getInstance(false));
					return planNode;
				})
				.map(planNode -> {
					try (CloseableIteration<? extends ValidationTuple, SailException> iterator = planNode.iterator()) {
						return new ValidationResultIterator(iterator, -1);
					}
				})
				.collect(Collectors.toList());

		return new LazyValidationReport(collect, -1);

	}

}
