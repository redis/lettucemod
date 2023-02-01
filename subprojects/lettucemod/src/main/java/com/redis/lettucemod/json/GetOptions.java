package com.redis.lettucemod.json;

import java.util.Optional;

import com.redis.lettucemod.protocol.JsonCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class GetOptions implements CompositeArgument {

	private Optional<String> indent = Optional.empty();
	private Optional<String> newline = Optional.empty();
	private Optional<String> space = Optional.empty();
	private boolean noEscape;

	public Optional<String> getIndent() {
		return indent;
	}

	public void setIndent(String indent) {
		this.indent = Optional.of(indent);
	}

	public Optional<String> getNewline() {
		return newline;
	}

	public void setNewline(String newline) {
		this.newline = Optional.of(newline);
	}

	public Optional<String> getSpace() {
		return space;
	}

	public void setSpace(String space) {
		this.space = Optional.of(space);
	}

	public boolean isNoEscape() {
		return noEscape;
	}

	public void setNoEscape(boolean noEscape) {
		this.noEscape = noEscape;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private String indent;
		private String newline;
		private String space;
		private boolean noEscape;

		public Builder indent(String indent) {
			this.indent = indent;
			return this;
		}

		public Builder newline(String newline) {
			this.newline = newline;
			return this;
		}

		public Builder space(String space) {
			this.space = space;
			return this;
		}

		public Builder noEscape() {
			return noEscape(true);
		}

		public Builder noEscape(boolean noEscape) {
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
		indent.ifPresent(i -> args.add(JsonCommandKeyword.INDENT).add(i));
		newline.ifPresent(l -> args.add(JsonCommandKeyword.NEWLINE).add(l));
		space.ifPresent(s -> args.add(JsonCommandKeyword.SPACE).add(s));
		if (noEscape) {
			args.add(JsonCommandKeyword.NOESCAPE);
		}
	}

}
