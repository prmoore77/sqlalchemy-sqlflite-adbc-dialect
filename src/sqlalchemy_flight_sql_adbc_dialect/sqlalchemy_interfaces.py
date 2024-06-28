# This module is used to bridge the gap between the different versions of SQLAlchemy.

import importlib_metadata
from typing import TypedDict, Optional, NotRequired, Any, Dict, List
from sqlalchemy.sql.type_api import TypeEngine


# Get the version of the package
package_name = "sqlalchemy"
package_version = importlib_metadata.version(package_name)

# Split the version into major, minor, and patch components
major, minor, patch = map(int, package_version.split('.')[:3])

# Conditional imports based on the version
if major > 1:
    from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedPrimaryKeyConstraint, \
        ReflectedForeignKeyConstraint, \
        ReflectedCheckConstraint
else:
    class ReflectedComputed(TypedDict):
        """Represent the reflected elements of a computed column, corresponding
        to the :class:`_schema.Computed` construct.

        The :class:`.ReflectedComputed` structure is part of the
        :class:`.ReflectedColumn` structure, which is returned by the
        :meth:`.Inspector.get_columns` method.

        """

        sqltext: str
        """the expression used to generate this column returned
        as a string SQL expression"""

        persisted: NotRequired[bool]
        """indicates if the value is stored in the table or computed on demand"""


    class ReflectedIdentity(TypedDict):
        """represent the reflected IDENTITY structure of a column, corresponding
        to the :class:`_schema.Identity` construct.

        The :class:`.ReflectedIdentity` structure is part of the
        :class:`.ReflectedColumn` structure, which is returned by the
        :meth:`.Inspector.get_columns` method.

        """

        always: bool
        """type of identity column"""

        on_null: bool
        """indicates ON NULL"""

        start: int
        """starting index of the sequence"""

        increment: int
        """increment value of the sequence"""

        minvalue: int
        """the minimum value of the sequence."""

        maxvalue: int
        """the maximum value of the sequence."""

        nominvalue: bool
        """no minimum value of the sequence."""

        nomaxvalue: bool
        """no maximum value of the sequence."""

        cycle: bool
        """allows the sequence to wrap around when the maxvalue
        or minvalue has been reached."""

        cache: Optional[int]
        """number of future values in the
        sequence which are calculated in advance."""

        order: bool
        """if true, renders the ORDER keyword."""

    class ReflectedColumn(TypedDict):
        """Dictionary representing the reflected elements corresponding to
        a :class:`_schema.Column` object.

        The :class:`.ReflectedColumn` structure is returned by the
        :class:`.Inspector.get_columns` method.

        """

        name: str
        """column name"""

        type: TypeEngine[Any]
        """column type represented as a :class:`.TypeEngine` instance."""

        nullable: bool
        """boolean flag if the column is NULL or NOT NULL"""

        default: Optional[str]
        """column default expression as a SQL string"""

        autoincrement: NotRequired[bool]
        """database-dependent autoincrement flag.

        This flag indicates if the column has a database-side "autoincrement"
        flag of some kind.   Within SQLAlchemy, other kinds of columns may
        also act as an "autoincrement" column without necessarily having
        such a flag on them.

        See :paramref:`_schema.Column.autoincrement` for more background on
        "autoincrement".

        """

        comment: NotRequired[Optional[str]]
        """comment for the column, if present.
        Only some dialects return this key
        """

        computed: NotRequired[ReflectedComputed]
        """indicates that this column is computed by the database.
        Only some dialects return this key.

        .. versionadded:: 1.3.16 - added support for computed reflection.
        """

        identity: NotRequired[ReflectedIdentity]
        """indicates this column is an IDENTITY column.
        Only some dialects return this key.

        .. versionadded:: 1.4 - added support for identity column reflection.
        """

        dialect_options: NotRequired[Dict[str, Any]]
        """Additional dialect-specific options detected for this reflected
        object"""


    class ReflectedConstraint(TypedDict):
        """Dictionary representing the reflected elements corresponding to
        :class:`.Constraint`

        A base class for all constraints
        """

        name: Optional[str]
        """constraint name"""

        comment: NotRequired[Optional[str]]
        """comment for the constraint, if present"""


    class ReflectedPrimaryKeyConstraint(ReflectedConstraint):
        """Dictionary representing the reflected elements corresponding to
        :class:`.PrimaryKeyConstraint`.

        The :class:`.ReflectedPrimaryKeyConstraint` structure is returned by the
        :meth:`.Inspector.get_pk_constraint` method.

        """

        constrained_columns: List[str]
        """column names which comprise the primary key"""

        dialect_options: NotRequired[Dict[str, Any]]
        """Additional dialect-specific options detected for this primary key"""

    class ReflectedForeignKeyConstraint(ReflectedConstraint):
        """Dictionary representing the reflected elements corresponding to
        :class:`.ForeignKeyConstraint`.

        The :class:`.ReflectedForeignKeyConstraint` structure is returned by
        the :meth:`.Inspector.get_foreign_keys` method.

        """

        constrained_columns: List[str]
        """local column names which comprise the foreign key"""

        referred_schema: Optional[str]
        """schema name of the table being referred"""

        referred_table: str
        """name of the table being referred"""

        referred_columns: List[str]
        """referred column names that correspond to ``constrained_columns``"""

        options: NotRequired[Dict[str, Any]]
        """Additional options detected for this foreign key constraint"""

    class ReflectedCheckConstraint(ReflectedConstraint):
        """Dictionary representing the reflected elements corresponding to
        :class:`.CheckConstraint`.

        The :class:`.ReflectedCheckConstraint` structure is returned by the
        :meth:`.Inspector.get_check_constraints` method.

        """

        sqltext: str
        """the check constraint's SQL expression"""

        dialect_options: NotRequired[Dict[str, Any]]
        """Additional dialect-specific options detected for this check constraint

        .. versionadded:: 1.3.8
        """


