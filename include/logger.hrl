-define(PRINT(Format, Args), io:format(Format, Args)).
-compile([{parse_transform, lager_transform}]).

-define(DEBUG(Format),
    begin lager:debug(Format, []), ok end).

-define(DEBUG(Format, Args),
    begin lager:debug(Format, Args), ok end).


-define(INFO(Format),
    begin lager:info(Format, []), ok end).

-define(INFO(Format, Args),
    begin lager:info(Format, Args), ok end).


-define(WARNING(Format),
    begin lager:warning(Format, []), ok end).

-define(WARNING(Format, Args),
    begin lager:warning([{fmt, Format}, {args, Args}], Format, Args), ok end).


-define(ERROR(Format),
    begin lager:error(Format, []), ok end).

-define(ERROR(Format, Args),
    begin lager:error([{fmt, Format}, {args, Args}], Format, Args), ok end).
