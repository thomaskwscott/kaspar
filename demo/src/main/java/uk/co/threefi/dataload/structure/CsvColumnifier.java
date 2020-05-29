package uk.co.threefi.dataload.structure;

import java.io.Serializable;

public class CsvColumnifier implements Columnifier, Serializable {

  private static final long serialVersionUID = -812004521983071103L;

  private String delimiter=",";

  public CsvColumnifier(String delimiter) {
    this.delimiter = delimiter;
  }
  public String[] toColumns(String raw) {
    return raw.split(delimiter);
  }
}
