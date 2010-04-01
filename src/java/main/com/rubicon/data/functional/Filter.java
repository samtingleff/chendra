package com.rubicon.data.functional;

import java.util.LinkedList;
import java.util.List;

public class Filter<E> {

	public List<E> filter(Iterable<E> values, Predicate<E>... predicates) {
		List<E> result = new LinkedList<E>();
		for (E e : values) {
			boolean accept = evaluate(e, predicates);
			if (accept)
				result.add(e);
		}
		return result;
	}

	public List<E> filter(Iterable<E> values, List<Predicate<E>> predicates) {
		List<E> result = new LinkedList<E>();
		for (E e : values) {
			boolean accept = evaluate(e, predicates);
			if (accept)
				result.add(e);
		}
		return result;
	}

	public boolean evaluate(E e, Predicate<E>... predicates) {
		boolean accept = true;
		for (Predicate<E> predicate : predicates)
			if (!predicate.evaluate(e)) {
				accept = false;
				break;
			}
		return accept;
	}

	public boolean evaluate(E e, List<Predicate<E>> predicates) {
		boolean accept = true;
		for (Predicate<E> predicate : predicates)
			if (!predicate.evaluate(e)) {
				accept = false;
				break;
			}
		return accept;
	}
}
