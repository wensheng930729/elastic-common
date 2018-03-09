package io.polyglotted.elastic.search;

import java.util.List;

public interface ScrollWalker<T> {
    boolean walk(List<T> results);
}