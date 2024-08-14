package io.malachai.datafaker;

import com.github.javafaker.Faker;
import java.util.concurrent.TimeUnit;

public class DataKindFaker {

    private final Faker faker = new Faker();

    public String get(String kind) {
        String[] params = kind.split("\\.");
        switch (params[0]) {
            case "address":
                if (params.length > 1) {
                    switch (params[1]) {
                        case "streetAddress":
                            return faker.address().streetAddress();
                        case "city":
                            return faker.address().city();
                        case "state":
                            return faker.address().state();
                        case "zip":
                            return faker.address().zipCode();
                    }
                }
                return faker.address().fullAddress().toString();
            case "date":
                if (params.length > 1) {
                    switch (params[1]) {
                        case "past":
                            return faker.date().past(Integer.parseInt(params[2]), TimeUnit.DAYS)
                                .toString();
                        case "future":
                            return faker.date().future(Integer.parseInt(params[2]), TimeUnit.DAYS)
                                .toString();
                    }
                }
                return faker.date().birthday().toString();
            case "internet":
                if (params.length > 1) {
                    switch (params[1]) {
                        case "avatar":
                            return faker.internet().avatar();
                        case "password":
                            return faker.internet().password();
                        case "email":
                            return faker.internet().emailAddress();
                        case "domain":
                            return faker.internet().domainName();
                        case "ipv4":
                            return faker.internet().ipV4Address();
                        case "ipv6":
                            return faker.internet().ipV6Address();
                        case "mac":
                            return faker.internet().macAddress();
                        case "url":
                            return faker.internet().url();
                    }
                }
                return faker.internet().uuid();
            case "phone":
                return faker.phoneNumber().phoneNumber();
            case "name":
                if (params.length > 1) {
                    switch (params[1]) {
                        case "first":
                            return faker.name().firstName();
                        case "last":
                            return faker.name().lastName();
                        case "bloodGroup":
                            return faker.name().bloodGroup();
                    }
                }
                return faker.name().fullName();
            case "job":
                if (params.length > 1) {
                    switch (params[1]) {
                        case "field":
                            return faker.job().field();
                        case "position":
                            return faker.job().position();
                        case "title":
                            return faker.job().title();
                    }
                }
                return faker.job().toString();
            default:
                return faker.lorem().characters(255);
        }
    }

}
