package com.msgbroker.util;

import com.msgbroker.util.generator.SecureIntGenerator;
import com.msgbroker.util.generator.SecureStringGenerator;

public interface Global {
    SecureStringGenerator SECURE_STRING_GENERATOR = new SecureStringGenerator();
    SecureIntGenerator SECURE_INT_GENERATOR = new SecureIntGenerator();
}
