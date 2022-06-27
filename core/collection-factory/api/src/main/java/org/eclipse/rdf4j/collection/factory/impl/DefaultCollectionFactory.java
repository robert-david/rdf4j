/*******************************************************************************
 * Copyright (c) 2022 Eclipse RDF4J contributors.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *******************************************************************************/
package org.eclipse.rdf4j.collection.factory.impl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.rdf4j.collection.factory.api.BindingSetKey;
import org.eclipse.rdf4j.collection.factory.api.CollectionFactory;
import org.eclipse.rdf4j.collection.factory.api.ValuePair;
import org.eclipse.rdf4j.common.exception.RDF4JException;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MutableBindingSet;

/**
 * A DefaultColelctionFactory that provides lists/sets/maps using standard common java in memory types
 */
public class DefaultCollectionFactory implements CollectionFactory {

	@Override
	public Set<Value> createValueSet() {
		return new HashSet<>();
	}

	@Override
	public Set<BindingSet> createSetOfBindingSets(Supplier<MutableBindingSet> supplier,
			Function<String, BiConsumer<Value, MutableBindingSet>> valueSetters) {
		return new HashSet<>();
	}

	@Override
	public <V> Map<Value, V> createValueKeyedMap() {
		return new HashMap<Value, V>();
	}

	@Override
	public void close() throws RDF4JException {
		// Nothing to do here.
	}

	@Override
	public <E> Map<BindingSetKey, E> createGroupByMap() {
		return new LinkedHashMap<>();
	}

	@Override
	public BindingSetKey createBindingSetKey(BindingSet bindingSet, List<Function<BindingSet, Value>> getValues) {
		Function<BindingSet, Integer> hashMaker = hashMaker(getValues);
		switch (getValues.size()) {
		case 1:
			return new DefaultBindingSetKey(List.of(getValues.get(0).apply(bindingSet)), hashMaker.apply(bindingSet));
		case 2:
			return new DefaultBindingSetKey(
					List.of(getValues.get(0).apply(bindingSet), getValues.get(1).apply(bindingSet)),
					hashMaker.apply(bindingSet));
		case 3:
			return new DefaultBindingSetKey(List.of(getValues.get(0).apply(bindingSet),
					getValues.get(1).apply(bindingSet), getValues.get(2).apply(bindingSet)),
					hashMaker.apply(bindingSet));
		case 4:
			return new DefaultBindingSetKey(
					List.of(getValues.get(0).apply(bindingSet), getValues.get(1).apply(bindingSet),
							getValues.get(2).apply(bindingSet), getValues.get(3).apply(bindingSet)),
					hashMaker.apply(bindingSet));
		default:
			List<Value> values = new ArrayList<>(getValues.size());
			for (Function<BindingSet, Value> getValue : getValues) {
				values.add(getValue.apply(bindingSet));
			}
			return new DefaultBindingSetKey(values, hashMaker.apply(bindingSet));
		}

	}

	private static Function<BindingSet, Integer> hashMaker(List<Function<BindingSet, Value>> getValues) {

		Function<BindingSet, Integer> hashFunction = (bs) -> {
			int nextHash = 0;
			for (Function<BindingSet, Value> getValue : getValues) {
				Value value = getValue.apply(bs);
				if (value != null) {
					nextHash = 31 * nextHash + value.hashCode();
				}
			}
			return nextHash;
		};
		return hashFunction;
	}

	@Override
	public ValuePair createValuePair(Value start, Value end) {
		return new DefaultValuePair(start, end);
	}

	@Override
	public Set<ValuePair> createValuePairSet() {
		return new HashSet<ValuePair>();
	}

	@Override
	public Queue<ValuePair> createValuePairQueue() {
		return new ArrayDeque<ValuePair>();
	}
}
