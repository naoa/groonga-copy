# Groonga copy

Groongaの異なるDB間のカラムの値をコピーするコマンドラインツール  
参照カラムの場合は、新しい値として参照テーブルにキーが追加される(IDが一致しなくても大丈夫)  
なお、キーや値のキャストはしていないので注意

## Compile

```
% make && make install
```

## Execute

```
% groonga-copy from_db_path from_table from_column to_db_path to_table to_column
```

## License

Public domain. You can copy and modify this project freely.
