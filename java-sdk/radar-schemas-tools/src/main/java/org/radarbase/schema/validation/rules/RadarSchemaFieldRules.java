package org.radarbase.schema.validation.rules;

import static org.radarbase.schema.validation.rules.RadarSchemaRules.validateDocumentation;
import static org.radarbase.schema.validation.rules.Validator.check;
import static org.radarbase.schema.validation.rules.Validator.matches;
import static org.radarbase.schema.validation.rules.Validator.valid;
import static org.radarbase.schema.validation.rules.Validator.validateNonNull;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.radarbase.schema.validation.ValidationException;

/**
 * Rules for RADAR-Schemas schema fields.
 */
public class RadarSchemaFieldRules implements SchemaFieldRules {
    private static final String UNKNOWN = "UNKNOWN";
    private static final List<String> FIELD_NAME_NOT_ALLOWED_SUFFIX = List.of(
            "value", "Value");

    // lowerCamelCase
    public static final Pattern FIELD_NAME_PATTERN = Pattern.compile(
            "^[a-z][a-z0-9]*([a-z0-9][A-Z][a-z0-9]+)?([A-Z][a-z0-9]+)*[A-Z]?$");

    private final Map<Schema.Type, Validator<SchemaField>> defaultsValidator;

    /**
     * Rules for RADAR-Schemas schema fields.
     */
    public RadarSchemaFieldRules() {
        defaultsValidator = new EnumMap<>(Schema.Type.class);
        defaultsValidator.put(Schema.Type.ENUM, this::validateDefaultEnum);
        defaultsValidator.put(Schema.Type.UNION, this::validateDefaultUnion);
    }

    @Override
    public Validator<SchemaField> validateFieldTypes(SchemaRules schemaRules) {
        return field -> {
            Schema schema = field.getField().schema();
            Schema.Type subType = schema.getType();
            if (subType == Schema.Type.UNION) {
                return validateInternalUnion(schemaRules).apply(field);
            } else if (subType == Schema.Type.RECORD) {
                return schemaRules.validateRecord().apply(schema);
            } else if (subType == Schema.Type.ENUM) {
                return schemaRules.validateEnum().apply(schema);
            } else {
                return valid();
            }
        };
    }

    @Override
    public Validator<SchemaField> validateDefault() {
        return input -> defaultsValidator
                .getOrDefault(input.getField().schema().getType(),
                        this::validateDefaultOther)
                .apply(input);
    }


    @Override
    public Validator<SchemaField> validateFieldName() {
        return validateNonNull(f -> f.getField().name(), matches(FIELD_NAME_PATTERN), message(
                "Field name does not respect lowerCamelCase name convention."
                        + " Please avoid abbreviations and write out the field name instead."))
                .and(validateNonNull(f -> f.getField().name(),
                        n -> FIELD_NAME_NOT_ALLOWED_SUFFIX.stream().noneMatch(n::endsWith),
                        message("Field name may not end with the following values: "
                                + FIELD_NAME_NOT_ALLOWED_SUFFIX + ".")));
    }

    @Override
    public Validator<SchemaField> validateFieldDocumentation() {
        return field -> validateDocumentation(field.getField().doc(),
                (m, f) -> message(m).apply(f), field);
    }


    private Stream<ValidationException> validateDefaultEnum(SchemaField field) {
        return check(!field.getField().schema().getEnumSymbols().contains(UNKNOWN)
                || field.getField().defaultVal() != null
                && field.getField().defaultVal().toString().equals(UNKNOWN),
                message("Default is \"" + field.getField().defaultVal()
                + "\". Any Avro enum type that has an \"UNKNOWN\" symbol must set its"
                + " default value to \"UNKNOWN\".").apply(field));
    }

    private Stream<ValidationException> validateDefaultUnion(SchemaField field) {
        return check(
                !field.getField().schema().getTypes().contains(Schema.create(Schema.Type.NULL))
                || field.getField().defaultVal() != null
                && field.getField().defaultVal().equals(JsonProperties.NULL_VALUE),
                message("Default is not null. Any nullable Avro field must"
                + " specify have its default value set to null.").apply(field));
    }

    private Stream<ValidationException> validateDefaultOther(SchemaField field) {
        return check(field.getField().defaultVal() == null, message(
                "Default of type " + field.getField().schema().getType() + " is set to "
                + field.getField().defaultVal() + ". The only acceptable default values are the"
                + " \"UNKNOWN\" enum symbol and null.").apply(field));
    }
}
