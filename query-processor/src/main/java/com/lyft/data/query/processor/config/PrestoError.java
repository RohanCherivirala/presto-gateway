package com.lyft.data.query.processor.config;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PrestoError {
  private String errorName;
  private int errorCode;
}
