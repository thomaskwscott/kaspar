package uk.co.threefi.dataload.structure;

import java.io.Serializable;

public class RawRow implements Serializable {
  private static final long serialVersionUID = -812004521983071103L;

  private String[] rawVals = null;

  public void setRawVals(String[] rawVals) {
    this.rawVals = rawVals;
  }

  public String getColumnVal(int columnIndex) {
    if(rawVals != null  && columnIndex < rawVals.length) {
      return rawVals[columnIndex];
    }
    return "";
  }

}
