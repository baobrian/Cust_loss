#!/usr/bin/env python
# coding=utf-8

import pandas as pd
import numpy as np
from sklearn import preprocessing
from sklearn import feature_extraction
import time


def getNowTime():
    return time.strftime("%Y%m%d%H%M",time.localtime(time.time()))




class DataPreprocess:

    def __init__(self):
        pass

    def num_replace_null(self,TRAIN,TEST=None,replace_type='nan'):
        '''
        针对标签库的数据为-1的值，将查询无数据的用中位数代替
        :param TRAIN:  待处理的数据
        :return:  TRAIN 处理完的数据
        '''

        numerical_df = TRAIN.select_dtypes(exclude=["object"])
        numerical_names=numerical_df.columns
        # 将nan值替换为0
        if replace_type=='nan':
            numerical_df=numerical_df.fillna(0)
            TRAIN[numerical_names]=numerical_df
        # 将-1 替换为0
        if replace_type=='minus':
            for element in numerical_names:
                temp = TRAIN[element].copy()
                numerical_df[element]=temp.map(lambda x : 0 if x==-1 else x)
            TRAIN[numerical_names] = numerical_df

        if replace_type=='mean':
            imputer = preprocessing.Imputer(missing_values="NaN",
                                                     strategy=replace_type, axis=0)
            TRAIN[numerical_names] = imputer.fit_transform(TRAIN[numerical_names])


        if TEST is not None:
            numerical_df_test = TEST.select_dtypes(exclude=["object"])
            numerical_names_test=numerical_df_test.columns
            if replace_type == 'nan':
                numerical_df_test = numerical_df_test.fillna(0)
                TEST[numerical_names_test] = numerical_df_test
            # 将-1 替换为0
            if replace_type == 'minus':
                for element in numerical_names_test:
                    temp = TEST[element].copy()
                    numerical_df_test[element] = temp.map(lambda x: 0 if x == -1 else x)
                TEST[numerical_names_test] = numerical_df_test
            return (TRAIN,TEST)
        else:
            return TRAIN




    def replace_na(self,TRAIN, TEST=None, fill_na_with="median"):

        '''
        将类别型数据的缺失值填充为missing，将提供几种策略来填充数值型的缺失值（"median", "mean", "most_frequent"）
        :param TRAIN:  待处理的训练集
        :param TEST:   待处理的测试集
        :param fill_na_with:  数据型的空值填充策略，默认为median
        :return: 若提供TEST则返回(TRAIN, TEST) ，否则返回 TRAIN (格式为:DataFrame)
        '''

        categorical = TRAIN.select_dtypes(include=["object"]).columns
        # numerical = TRAIN.select_dtypes(exclude=["object"]).columns
        # numerical_test = TEST.select_dtypes(exclude=["object"]).columns

        TRAIN[categorical] = TRAIN[categorical].fillna("missing")
        # if len(numerical) > 0:
        #     if isinstance(fill_na_with, str):
        #         if fill_na_with not in ["median", "mean", "most_frequent"]:
        #             error = ("'" + fill_na_with + "'" +
        #                      " is not an available strategy!\n" +
        #                      "You should use one of the following: " +
        #                      "'median', 'mean', 'most_frequent' or a float.")
        #             raise Exception(error)
        #         else:
        #             imputer = preprocessing.Imputer(missing_values="NaN",
        #                                             strategy=fill_na_with, axis=0)
        #             TRAIN[numerical] = imputer.fit_transform(TRAIN[numerical])
        #     else:
        #         TRAIN[numerical] = TRAIN[numerical].fillna(value=fill_na_with)
        if TEST is not None:
            TEST[categorical] = TEST[categorical].fillna("missing")
            # if len(numerical_test) > 0:
            #     if isinstance(fill_na_with, str):
            #         TEST[numerical_test] = imputer.transform(TEST[numerical_test])
            #     else:
            #         TEST[numerical_test] = TEST[numerical_test].fillna(value=fill_na_with)
            print (' 空值处理完成')
            return (TRAIN, TEST)
        else:
            print (' 空值处理完成')
            return TRAIN


    def transform_categorical_alphabetically(self,TRAIN, TEST=None, verbose=0):


        '''
        将类别型特征转化成数字，0对应第一个，1对应第二个,以此类推
        :param TRAIN: 待处理的训练集
        :param TEST: 待处理的测试数据集
        :param verbose:
        :return:  若提供TEST则返回(TRAIN, TEST) ，否则返回 TRAIN (格式为:DataFrame)
        '''

        categorical = TRAIN.select_dtypes(include=["object"]).columns
        le = preprocessing.LabelEncoder()
        if TEST is not None:
            if len(categorical) > 0:
                for col in categorical:
                    TRAIN[col] = le.fit_transform(TRAIN[col])
                    TEST[col] = le.transform(TEST[col])
                    if verbose > 0:
                        print("\n-----\n")
                        print("Feature: {0}".format(col))
                        if verbose > 1:
                            for i in range(len(le.classes_)):
                                print("{0}: {1}".format(le.classes_[i],
                                                        np.sort(TRAIN[col].unique())[i]))
            print (' 特征转码完成')
            return (TRAIN, TEST)
        else:
            map_dict=[]
            if len(categorical) > 0:
                for col in categorical:
                    TRAIN[col] = le.fit_transform(TRAIN[col])
                    map_dict.append((col,dict([(name, i) for i, name in enumerate(le.classes_)])))
                    if verbose > 0:
                        print("\n-----\n")
                        print("Feature: {0}".format(col))
                        if verbose > 1:
                            for i in range(len(le.classes_)):
                                print("{0}: {1}".format(le.classes_[i],
                                                        np.sort(TRAIN[col].unique())[i]))

            print (' 特征转码完成')
            return (TRAIN,dict(map_dict))


    def transform_categorical_sorted_by_count(self,TRAIN, TEST=None,handle_unknown="error",verbose=0):
        '''
        将类别型特征转化成数字，0对应频次最高的类别，1对应第二频次最高的类别,以此类推


        :param TRAIN:
        :param TEST:
        :param handle_unknown: 处理未知数据的策略，分为 error  ignore  NaN，error抛出一个错误，其他为用NA代替
        :param verbose:
        :return: 若提供TEST则返回(TRAIN, TEST) ，否则返回 TRAIN (格式为:DataFrame)
        '''

        categorical = TRAIN.select_dtypes(include=["object"]).columns

        if TEST is not None:
            if len(categorical) > 0:
                for col in categorical:
                    cat_counts = TRAIN[col].value_counts(dropna=False)
                    dict_cat_counts = dict(zip(cat_counts.index,
                                               range(len(cat_counts))))
                    not_in_train = list(set(TEST[col].unique()) -
                                        set(cat_counts.index))
                    if len(not_in_train) > 0:
                        if handle_unknown == "error":
                            raise ValueError("TEST contains new labels: {0} "
                                             "in variable {1}."
                                             .format(not_in_train, col))
                        if handle_unknown == "ignore":
                            print("\n-----\n")
                            print("Variable: {0}".format(col))
                            print("Unknown category(ies) {0} present during "
                                  "transform has(ve) been ignored."
                                  .format(not_in_train))
                        if handle_unknown == "NaN":
                            print("\n-----\n")
                            print("Variable: {0}".format(col))
                            print("Unknown category(ies) {0} present during "
                                  "transform has(ve) been replaced by NA."
                                  .format(not_in_train))
                            for item in not_in_train:
                                dict_cat_counts[item] = np.nan
                    TRAIN[col] = TRAIN[col].replace(dict_cat_counts)
                    TEST[col] = TEST[col].replace(dict_cat_counts)
                    if verbose > 0:
                        print("\n-----\n")
                        print("Feature: {0}".format(col))
                        if verbose > 1:
                            for i in range(len(TRAIN[col].unique())):
                                print("{0}: {1}".format(cat_counts.index[i], i))
            return (TRAIN, TEST)
        else:
            if len(categorical) > 0:
                for col in categorical:
                    cat_counts = TRAIN[col].value_counts(dropna=False)
                    dict_cat_counts = dict(zip(cat_counts.index,
                                               range(len(cat_counts))))
                    TRAIN[col] = TRAIN[col].replace(dict_cat_counts)
                    if verbose > 0:
                        print("\n-----\n")
                        print("Feature: {0}".format(col))
                        if verbose > 1:
                            for i in range(len(TRAIN[col].unique())):
                                print("{0}: {1}".format(cat_counts.index[i], i))
            return TRAIN



    def transform_categorical_to_dummy(self,TRAIN, TEST=None):
        """
        Transform categorical features to dummy variables.
        Having categories in transform that are not present during training
        will raise an error (which is the behavior of the scikit-learn's
        transformers).
        -----
        Arguments:
            TRAIN: DataFrame.
            TEST: DataFrame, optional (default=None).
        -----
        Returns:
            TRAIN: DataFrame.
            TEST: DataFrame.
                This second DataFrame is returned if two DataFrames were provided.
        """
        categorical = TRAIN.select_dtypes(include=["object"]).columns
        numerical = TRAIN.select_dtypes(exclude=["object"]).columns
        dv = feature_extraction.DictVectorizer(sparse=False)
        test_numerical=TEST.select_dtypes(include=['object']).columns

        TRAIN = pd.concat([pd.DataFrame(dv.fit_transform(TRAIN[categorical]
                                                         .to_dict("records"))), TRAIN[numerical]], axis=1)
        features_names = dv.get_feature_names()
        TRAIN.columns = [features_names + list(numerical)]
        TRAIN.columns = TRAIN.columns.str.replace("=", "_")
        if TEST is not None:
            TEST = pd.concat([pd.DataFrame(dv.transform(TEST[categorical]
                                                        .to_dict("records"))), TEST[test_numerical]], axis=1)
            TEST.columns = [features_names + list(test_numerical)]
            TEST.columns = TEST.columns.str.replace("=", "_")
            return (TRAIN, TEST)
        else:
            return TRAIN

    def transform_categorical_by_count(self,TRAIN, TEST=None, handle_unknown="error",
                                       verbose=0):
        """
        Transform categorical features to numerical. The categories are encoded
        by their respective count (in the TRAIN dataset).
        To be consistent with scikit-learn transformers having categories
        in transform that are not present during training will raise an error
        by default.
        -----
        Arguments:
            TRAIN: DataFrame.
            TEST: DataFrame, optional (default=None).
            handle_unknown: str, "error", "ignore" or "NaN",
            optional (default="error").
                Whether to raise an error, ignore or replace by NA if an unknown
                category is present during transform.
            verbose: integer, optional (default=0).
                Controls the verbosity of the process.
        -----
        Returns:
            TRAIN: DataFrame.
            TEST: DataFrame.
                This second DataFrame is returned if two DataFrames were provided.
        """
        categorical = TRAIN.select_dtypes(include=["object"]).columns

        if TEST is not None:
            if len(categorical) > 0:
                for col in categorical:
                    cat_counts = TRAIN[col].value_counts(dropna=False)
                    dict_cat_counts = dict(zip(cat_counts.index, cat_counts))
                    not_in_train = list(set(TEST[col].unique()) -
                                        set(cat_counts.index))
                    if len(not_in_train) > 0:
                        if handle_unknown == "error":
                            raise ValueError("TEST contains new labels: {0} "
                                             "in variable {1}."
                                             .format(not_in_train, col))
                        if handle_unknown == "ignore":
                            print("\n-----\n")
                            print("Variable: {0}".format(col))
                            print("Unknown category(ies) {0} present during "
                                  "transform has(ve) been ignored."
                                  .format(not_in_train))
                        if handle_unknown == "NaN":
                            print("\n-----\n")
                            print("Variable: {0}".format(col))
                            print("Unknown category(ies) {0} present during "
                                  "transform has(ve) been replaced by NA."
                                  .format(not_in_train))
                            for item in not_in_train:
                                dict_cat_counts[item] = np.nan
                    TRAIN[col] = TRAIN[col].replace(dict_cat_counts)
                    TEST[col] = TEST[col].replace(dict_cat_counts)
                    if verbose > 0:
                        print("\n-----\n")
                        print("Feature: {0}".format(col))
                        if verbose > 1:
                            print(cat_counts)
            return (TRAIN, TEST)
        else:
            if len(categorical) > 0:
                for col in categorical:
                    cat_counts = TRAIN[col].value_counts(dropna=False)
                    dict_cat_counts = dict(zip(cat_counts.index, cat_counts))
                    TRAIN[col] = TRAIN[col].replace(dict_cat_counts)
                    if verbose > 0:
                        print("\n-----\n")
                        print("Feature: {0}".format(col))
                        if verbose > 1:
                            print(cat_counts)
            return TRAIN


    def transform_categorical_by_percentage(self,TRAIN, TEST=None,handle_unknown="error", verbose=0):
        """
        Transform categorical features to numerical. The categories are encoded
        by their relative frequency (in the TRAIN dataset).
        To be consistent with scikit-learn transformers having categories
        in transform that are not present during training will raise an error
        by default.
        -----
        Arguments:
            TRAIN: DataFrame.
            TEST: DataFrame, optional (default=None).
            handle_unknown: str, "error", "ignore" or "NaN",
            optional (default="error").
                Whether to raise an error, ignore or replace by NA if an unknown
                category is present during transform.
            verbose: integer, optional (default=0).
                Controls the verbosity of the process.
        -----
        Returns:
            TRAIN: DataFrame.
            TEST: DataFrame.
                This second DataFrame is returned if two DataFrames were provided.
        """
        categorical = TRAIN.select_dtypes(include=["object"]).columns

        if TEST is not None:
            if len(categorical) > 0:
                for col in categorical:
                    cat_counts = TRAIN[col].value_counts(normalize=True,
                                                         dropna=False)
                    dict_cat_counts = dict(zip(cat_counts.index, cat_counts))
                    not_in_train = list(set(TEST[col].unique()) -
                                        set(cat_counts.index))
                    if len(not_in_train) > 0:
                        if handle_unknown == "error":
                            raise ValueError("TEST contains new labels: {0} "
                                             "in variable {1}."
                                             .format(not_in_train, col))
                        if handle_unknown == "ignore":
                            print("\n-----\n")
                            print("Variable: {0}".format(col))
                            print("Unknown category(ies) {0} present during "
                                  "transform has(ve) been ignored."
                                  .format(not_in_train))
                        if handle_unknown == "NaN":
                            print("\n-----\n")
                            print("Variable: {0}".format(col))
                            print("Unknown category(ies) {0} present during "
                                  "transform has(ve) been replaced by NA."
                                  .format(not_in_train))
                            for item in not_in_train:
                                dict_cat_counts[item] = np.nan
                    TRAIN[col] = TRAIN[col].replace(dict_cat_counts)
                    TEST[col] = TEST[col].replace(dict_cat_counts)
                    if verbose > 0:
                        print("\n-----\n")
                        print("Feature: {0}".format(col))
                        if verbose > 1:
                            print(cat_counts)
            return (TRAIN, TEST)
        else:
            for col in categorical:
                cat_counts = TRAIN[col].value_counts(normalize=True, dropna=False)
                dict_cat_counts = dict(zip(cat_counts.index, cat_counts))
                TRAIN[col] = TRAIN[col].replace(dict_cat_counts)
                if verbose > 0:
                    print("\n-----\n")
                    print("Feature: {0}".format(col))
                    if verbose > 1:
                        print(cat_counts)
            return TRAIN



    def transform_numerical_to_quantiles(self,TRAIN, TEST=None, n_quantiles=10):
        """
        Transform numerical features to quantiles.
        -----
        Arguments:
            TRAIN: DataFrame.
            TEST: DataFrame, optional (default=None).
            n_quantiles: integer, optional (default=10).
                Number of quantiles.
        -----
        Returns:
            TRAIN: DataFrame.
            TEST: DataFrame.
                This second DataFrame is returned if two DataFrames were provided.
        """
        numerical = TRAIN.select_dtypes(exclude=["object"]).columns

        if TEST is not None:
            if len(numerical) > 0:
                for col in numerical:
                    TRAIN[col], bins = pd.qcut(TRAIN[col], n_quantiles,
                                               labels=False, retbins=True)
                    TEST["_bins_" + col] = np.nan
                    for idx, item in enumerate(bins):
                        if idx <= 1:
                            TEST["_bins_" + col] = np.where(TEST[col] <= item, 0,
                                                            TEST["_bins_" + col])
                        if idx >= (n_quantiles - 1):
                            TEST["_bins_" + col] = np.where(TEST[col] > item,
                                                            (n_quantiles - 1),
                                                            TEST["_bins_" + col])
                        if (1 < idx) & (idx <= (n_quantiles - 1)):
                            TEST["_bins_" + col] = np.where((float(bins[idx - 1]) <
                                                             TEST[col]) &
                                                            (TEST[col] <= item),
                                                            idx - 1,
                                                            TEST["_bins_" + col])
                TEST = TEST.drop(numerical, axis=1)
                TEST.columns = TEST.columns.str.replace("_bins_", "")
            return (TRAIN, TEST)
        else:
            if len(numerical) > 0:
                for col in numerical:
                    TRAIN[col] = pd.qcut(TRAIN[col], n_quantiles, labels=False)
            return TRAIN



