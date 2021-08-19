package org.apache.drill.exec.store.ipfs.formats.text.compliant;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.List;

@JsonTypeName("text") @JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class TextFormatConfig implements FormatPluginConfig {

  public List<String> extensions = ImmutableList.of();
  public String lineDelimiter = "\n";
  public char fieldDelimiter = ',';
  public char quote = '"';
  public char escape = '"';
  public char comment = '#';
  public boolean skipFirstLine = false;
  public boolean extractHeader = false;

  public List<String> getExtensions() {
    return extensions;
  }

  public char getQuote() {
    return quote;
  }

  public char getEscape() {
    return escape;
  }

  public char getComment() {
    return comment;
  }

  public String getLineDelimiter() {
    return lineDelimiter;
  }

  public char getFieldDelimiter() {
    return fieldDelimiter;
  }

  @JsonIgnore
  public boolean isHeaderExtractionEnabled() {
    return extractHeader;
  }

  @JsonIgnore
  public String getFieldDelimiterAsString(){
    return new String(new char[]{fieldDelimiter});
  }

  @Deprecated
  @JsonProperty("delimiter")
  public void setFieldDelimiter(char delimiter){
    this.fieldDelimiter = delimiter;
  }

  public boolean isSkipFirstLine() {
    return skipFirstLine;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + comment;
    result = prime * result + escape;
    result = prime * result + ((extensions == null) ? 0 : extensions.hashCode());
    result = prime * result + fieldDelimiter;
    result = prime * result + ((lineDelimiter == null) ? 0 : lineDelimiter.hashCode());
    result = prime * result + quote;
    result = prime * result + (skipFirstLine ? 1231 : 1237);
    result = prime * result + (extractHeader ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TextFormatConfig other = (TextFormatConfig) obj;
    if (comment != other.comment) {
      return false;
    }
    if (escape != other.escape) {
      return false;
    }
    if (extensions == null) {
      if (other.extensions != null) {
        return false;
      }
    } else if (!extensions.equals(other.extensions)) {
      return false;
    }
    if (fieldDelimiter != other.fieldDelimiter) {
      return false;
    }
    if (lineDelimiter == null) {
      if (other.lineDelimiter != null) {
        return false;
      }
    } else if (!lineDelimiter.equals(other.lineDelimiter)) {
      return false;
    }
    if (quote != other.quote) {
      return false;
    }
    if (skipFirstLine != other.skipFirstLine) {
      return false;
    }
    if (extractHeader != other.extractHeader) {
      return false;
    }
    return true;
  }



}