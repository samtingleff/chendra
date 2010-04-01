package com.rubicon.data.functional;

import java.util.LinkedList;
import java.util.List;

public class Filter<E> {

	public List<E> filter(Iterable<E> values, Predicate<E>... predicates) {
		List<E> result = new LinkedList<E>();
		for (E e : values) {
			boolean accept = true;
			for (Predicate<E> predicate : predicates)
				if (!predicate.evaluate(e)) {
					accept = false;
					break;
				}
			if (accept)
				result.add(e);
		}
		return result;
	}

	public List<E> filter(Iterable<E> values, List<Predicate<E>> predicates) {
		List<E> result = new LinkedList<E>();
		for (E e : values) {
			boolean accept = true;
			for (Predicate<E> predicate : predicates)
				if (!predicate.evaluate(e)) {
					accept = false;
					break;
				}
			if (accept)
				result.add(e);
		}
		return result;
	}
}
