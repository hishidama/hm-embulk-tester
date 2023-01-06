package com.hishidama.embulk.tester;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@SuppressWarnings("serial")
public class EmbulkTestParserConfig extends LinkedHashMap<String, Object> {

	public void setType(String type) {
		set("type", type);
	}

	public void set(String key, Object value) {
		if (value == null) {
			super.remove(key);
		} else {
			super.put(key, value);
		}
	}

	@SuppressWarnings("unchecked")
	public List<EmbulkTestColumn> getColumns() {
		return (List<EmbulkTestColumn>) super.computeIfAbsent("columns", k -> new ArrayList<>());
	}

	public EmbulkTestColumn addColumn(String name, String type) {
		EmbulkTestColumn column = new EmbulkTestColumn();
		column.set("name", name);
		column.set("type", type);
		getColumns().add(column);
		return column;
	}

	public static class EmbulkTestColumn extends LinkedHashMap<String, Object> {

		public EmbulkTestColumn set(String key, Object value) {
			if (value == null) {
				super.remove(key);
			} else {
				super.put(key, value);
			}
			return this;
		}
	}
}
