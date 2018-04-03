# To-Do

## Entity

- [x] Capture property value changes, but only if it is initialized with a value
	- [x] Add an `original_value` method to the property objects
	- [x] Add a `changed` property to the Properties manager that collects changes in the properties
	- [x] Write tests to support property changes

## Mapper

- [ ] Define relationships
	- [ ] Query based on relationship with or without entity|collection
- [ ] Property validations that raise an exception before saving
- [ ] on_property change methods that run before saving after validations

## Connection

- [ ] Build main connection interface
- [ ] Build response object
	- [ ] Build response collection that subclasses `entity.Collection`
