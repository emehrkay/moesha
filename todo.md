# To-Do

## Entity

- [x] Capture property value changes, but only if it is initialized with a value
    - [x] Add an `original_value` method to the property objects
    - [x] Add a `changed` property to the Properties manager that collects changes in the properties
    - [x] Write tests to support property changes

## Mapper

- [x] Define relationships
    - [ ] Query based on relationship with or without entity|collection
- [ ] Property validations that raise an exception before saving
- [x] on_property change methods that run before saving after validations
- [x] on_relationship_$name_added custom events
- [ ] on_relationship_$name_updted custom events
- [ ] on_relationship_$name_removed custom events

## Connection

- [x] Build main connection interface
    - [x] Raise exception when context is not set
- [x] Build response object
    - [x] Build response collection that subclasses `entity.Collection`

# Miscellany

- [ ] Write documentation
- [ ] Fix existing unit tests
    - [ ] Add new tests to acheive 100% coverage
