package com.redis.lettucemod.json;

import com.redis.lettucemod.protocol.JsonCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class GetOptions implements CompositeArgument {

	private String indent;
	private String newline;
	private String space;
	private boolean noEscape;

	public String getIndent() {
		return indent;
	}

	public void setIndent(String indent) {
		this.indent = indent;
	}

	public String getNewline() {
		return newline;
	}

	public void setNewline(String newline) {
		this.newline = newline;
	}

	public String getSpace() {
		return space;
	}

	public void setSpace(String space) {
		this.space = space;
	}

	public boolean isNoEscape() {
		return noEscape;
	}

	public void setNoEscape(boolean noEscape) {
		this.noEscape = noEscape;
	}

	public static GetOptionsBuilder builder() {
		return new GetOptionsBuilder();
	}

	public static class GetOptionsBuilder {
		private String indent;
		private String newline;
		private String space;
		private boolean noEscape;

		public GetOptions.GetOptionsBuilder indent(String indent) {
			this.indent = indent;
			return this;
		}

		public GetOptions.GetOptionsBuilder newline(String newline) {
			this.newline = newline;
			return this;
		}

		public GetOptions.GetOptionsBuilder space(String space) {
			this.space = space;
			return this;
		}

		public GetOptions.GetOptionsBuilder noEscape(boolean noEscape) {
			this.noEscape = noEscape;
			return this;
		}

		public GetOptions build() {
			GetOptions options = new GetOptions();
			options.setIndent(indent);
			options.setNewline(newline);
			options.setSpace(space);
			options.setNoEscape(noEscape);
			return options;
		}
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		if (indent != null) {
			args.add(JsonCommandKeyword.INDENT);
			args.add(indent);
		}
		if (newline != null) {
			args.add(JsonCommandKeyword.NEWLINE);
			args.add(newline);
		}
		if (space != null) {
			args.add(JsonCommandKeyword.SPACE);
			args.add(space);
		}
		if (noEscape) {
			args.add(JsonCommandKeyword.NOESCAPE);
		}
	}

}
